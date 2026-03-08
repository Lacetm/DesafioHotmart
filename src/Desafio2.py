from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DAY_OFFSET      = int(os.getenv("PROCESSING_DAY_OFFSET", 1))
PROCESSING_DATE = date.today() - timedelta(days=DAY_OFFSET)  # D-1

PATH_PURCHASE     = os.getenv("PATH_PURCHASE",     "data/purchase")
PATH_PRODUCT_ITEM = os.getenv("PATH_PRODUCT_ITEM", "data/product_item")
PATH_EXTRA_INFO   = os.getenv("PATH_EXTRA_INFO",   "data/purchase_extra_info")
PATH_GMV_SNAPSHOT = os.getenv("PATH_GMV_SNAPSHOT", "data/gmv_daily_snapshot")

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "ETL_GMV_Diario"))
        .master(os.getenv("SPARK_MASTER",    "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

DDL_GMV_SNAPSHOT = """
    CREATE TABLE IF NOT EXISTS gmv_daily_snapshot (
        snapshot_date   DATE            NOT NULL COMMENT 'Dia em que o snapshot foi gerado (D-1)',
        reference_date  DATE            NOT NULL COMMENT 'Dia do GMV - order_date da compra',
        subsidiary      STRING          COMMENT 'Subsidiária (NULL = ainda não chegou)',
        gmv             DECIMAL(18, 2)  NOT NULL COMMENT 'Soma do purchase_value das transações liberadas',
        is_current      BOOLEAN         NOT NULL COMMENT 'TRUE = registro corrente, FALSE = histórico invalidado',
        created_at      TIMESTAMP       COMMENT 'Timestamp de inserção do registro'
    )
    USING DELTA
    PARTITIONED BY (reference_date)
    COMMENT 'Snapshot histórico e imutável do GMV diário por subsidiária'
"""


def create_table(spark: SparkSession):
    spark.sql(DDL_GMV_SNAPSHOT)
    print("[OK] Tabela gmv_daily_snapshot verificada/criada.")


def load_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    purchase     = spark.read.format("delta").load(PATH_PURCHASE)
    product_item = spark.read.format("delta").load(PATH_PRODUCT_ITEM)
    extra_info   = spark.read.format("delta").load(PATH_EXTRA_INFO)

    return purchase, product_item, extra_info

def deduplicate_cdc(df: DataFrame, key_col: str) -> DataFrame:
    w = Window.partitionBy(key_col).orderBy(F.desc("transaction_datetime"))

    return (
        df
        .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def consolidate_sources(
    purchase: DataFrame,
    product_item: DataFrame,
    extra_info: DataFrame
) -> DataFrame:
    return (
        purchase
        .join(
            product_item.select("purchase_id", "purchase_value", "product_id"),
            on="purchase_id",
            how="left"
        )
        .join(
            extra_info.select("purchase_id", "subsidiary"),
            on="purchase_id",
            how="left"
        )
    )


def calculate_gmv(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("release_date").isNotNull())
        .withColumn("purchase_value",
                    F.coalesce(F.col("purchase_value"), F.lit(0)))
        .withColumn("subsidiary",
                    F.coalesce(F.col("subsidiary"), F.lit("desconhecida")))
        .withColumn("order_date", F.to_date("order_date"))
        .groupBy(
            F.col("order_date").alias("reference_date"),
            F.col("subsidiary")
        )
        .agg(F.sum("purchase_value").cast("decimal(18,2)").alias("gmv"))
    )

def load_snapshot(spark: SparkSession, gmv: DataFrame, snapshot_date: date):
    from delta.tables import DeltaTable

    if gmv.isEmpty():
        print("[AVISO] Nenhum dado de GMV para gravar.")
        return

    new_snapshot = (
        gmv
        .withColumn("snapshot_date", F.lit(str(snapshot_date)).cast("date"))
        .withColumn("is_current",    F.lit(True))
        .withColumn("created_at",    F.current_timestamp())
    )

    reference_dates = [
        r["reference_date"]
        for r in gmv.select("reference_date").distinct().collect()
    ]

    if DeltaTable.isDeltaTable(spark, PATH_GMV_SNAPSHOT):
        delta_table = DeltaTable.forPath(spark, PATH_GMV_SNAPSHOT)

        delta_table.update(
            condition=F.col("reference_date").isin(reference_dates) & F.col("is_current"),
            set={"is_current": F.lit(False)}
        )

        new_snapshot.write.format("delta").mode("append").save(PATH_GMV_SNAPSHOT)

    else:
        # Primeira carga
        new_snapshot.write \
            .format("delta") \
            .partitionBy("reference_date") \
            .mode("overwrite") \
            .save(PATH_GMV_SNAPSHOT)

    print(f"[OK] Snapshot gravado | snapshot_date={snapshot_date} | {new_snapshot.count()} linhas")

def query_current_gmv(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        SELECT
            reference_date,
            subsidiary,
            gmv
        FROM gmv_daily_snapshot
        WHERE is_current = TRUE
        ORDER BY reference_date, subsidiary
    """)


def query_historical_gmv(spark: SparkSession, snapshot_date: str) -> DataFrame:
    return spark.sql(f"""
        SELECT
            snapshot_date,
            reference_date,
            subsidiary,
            gmv
        FROM gmv_daily_snapshot
        WHERE is_current = TRUE
          AND snapshot_date = '{snapshot_date}'
        ORDER BY reference_date, subsidiary
    """)

def run_etl():
    spark = get_spark()

    print("=" * 60)
    print(f"  ETL GMV - Processando D-1: {PROCESSING_DATE}")
    print("=" * 60)

    create_table(spark)

    print("\n[1/4] Carregando dados das fontes...")
    purchase_raw, product_item_raw, extra_info_raw = load_data(spark)

    print("[2/4] Deduplicando eventos CDC...")
    purchase     = deduplicate_cdc(purchase_raw,     "purchase_id")
    product_item = deduplicate_cdc(product_item_raw, "purchase_id")
    extra_info   = deduplicate_cdc(extra_info_raw,   "purchase_id")

    print("[3/4] Consolidando fontes e calculando GMV...")
    consolidated = consolidate_sources(purchase, product_item, extra_info)
    gmv          = calculate_gmv(consolidated)

    print("[4/4] Gravando snapshot histórico...")
    load_snapshot(spark, gmv, PROCESSING_DATE)

    print("\n--- GMV corrente (is_current=TRUE) ---")
    spark.read.format("delta").load(PATH_GMV_SNAPSHOT) \
        .createOrReplaceTempView("gmv_daily_snapshot")
    query_current_gmv(spark).show()

    print("\n[CONCLUÍDO]")
    spark.stop()


if __name__ == "__main__":
    run_etl()