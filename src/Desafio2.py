from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DAY_OFFSET      = int(os.getenv("PROCESSING_DAY_OFFSET", 1))
PROCESSING_DATE = date.today() - timedelta(days=DAY_OFFSET)

PATH_PURCHASE     = os.getenv("PATH_PURCHASE",     "data/purchase")
PATH_PRODUCT_ITEM = os.getenv("PATH_PRODUCT_ITEM", "data/product_item")
PATH_EXTRA_INFO   = os.getenv("PATH_EXTRA_INFO",   "data/purchase_extra_info")
PATH_GMV_SNAPSHOT = os.getenv("PATH_GMV_SNAPSHOT", "data/gmv_daily_snapshot")

DDL_GMV_SNAPSHOT = """
    CREATE TABLE IF NOT EXISTS gmv_daily_snapshot (
        snapshot_date   DATE            NOT NULL,
        reference_date  DATE            NOT NULL,
        subsidiary      STRING,
        gmv             DECIMAL(18, 2)  NOT NULL,
        is_current      BOOLEAN         NOT NULL,
        created_at      TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (reference_date)
"""


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "ETL_GMV_Diario"))
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def create_table(spark: SparkSession):
    spark.sql(DDL_GMV_SNAPSHOT)


def load_sample_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    purchase_raw = spark.createDataFrame([
        ("2023-01-20 22:00:00", "2023-01-20", 55, 15947,  5,      "2023-01-20", "2023-01-20", 852852),
        ("2023-01-26 00:01:00", "2023-01-26", 56, 369798, 746520, "2023-01-26", None,         963963),
        ("2023-02-05 10:00:00", "2023-02-05", 55, 160001, 5,      "2023-01-20", "2023-01-20", 852852),
        ("2023-02-26 03:00:00", "2023-02-26", 69, 18,     160001, "2023-02-26", "2023-02-28", 96967),
        ("2023-07-15 09:00:00", "2023-07-15", 55, 160001, 5,      "2023-01-20", "2023-03-01", 852852),
    ], ["transaction_datetime", "transaction_date", "purchase_id",
        "buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"])

    product_item_raw = spark.createDataFrame([
        ("2023-01-20 22:02:00", "2023-01-20", 55, 696969, 10,  50.00),
        ("2023-01-25 23:59:59", "2023-01-25", 56, 808080, 120, 2400.00),
        ("2023-02-26 03:00:00", "2023-02-26", 69, 373737, 2,   2000.00),
        ("2023-07-12 09:00:00", "2023-07-12", 55, 696969, 10,  55.00),
    ], ["transaction_datetime", "transaction_date", "purchase_id",
        "product_id", "item_quantity", "purchase_value"])

    extra_info_raw = spark.createDataFrame([
        ("2023-01-23 00:05:00", "2023-01-23", 55, "nacional"),
        ("2023-01-25 23:59:59", "2023-01-25", 56, "internacional"),
        ("2023-02-28 01:10:00", "2023-02-28", 69, "nacional"),
        ("2023-03-12 07:00:00", "2023-03-12", 69, "internacional"),
    ], ["transaction_datetime", "transaction_date", "purchase_id", "subsidiary"])

    return purchase_raw, product_item_raw, extra_info_raw


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
        .withColumn("purchase_value", F.coalesce(F.col("purchase_value"), F.lit(0)))
        .withColumn("subsidiary",     F.coalesce(F.col("subsidiary"),     F.lit("desconhecida")))
        .withColumn("order_date",     F.to_date("order_date"))
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


def run_etl(use_sample_data: bool = True):
    spark = get_spark()
    print(f"  ETL GMV: {PROCESSING_DATE}")


    create_table(spark)

    print("\n[1/4] Carregando dados...")
    if use_sample_data:
        purchase_raw, product_item_raw, extra_info_raw = load_sample_data(spark)
    else:
        purchase_raw, product_item_raw, extra_info_raw = load_data(spark)

    print("[2/4] Deduplicando eventos...")
    purchase     = deduplicate_cdc(purchase_raw,     "purchase_id")
    product_item = deduplicate_cdc(product_item_raw, "purchase_id")
    extra_info   = deduplicate_cdc(extra_info_raw,   "purchase_id")

    print("[3/4] Calculando GMV...")
    consolidated = consolidate_sources(purchase, product_item, extra_info)
    gmv          = calculate_gmv(consolidated)

    print("\n--- GMV calculado neste processamento ---")
    gmv.show()

    print("[4/4] Gravando snapshot...")
    load_snapshot(spark, gmv, PROCESSING_DATE)

    print("\n--- GMV corrente (is_current=TRUE) ---")
    spark.read.format("delta").load(PATH_GMV_SNAPSHOT) \
        .createOrReplaceTempView("gmv_daily_snapshot")
    query_current_gmv(spark).show()

    print("\n[CONCLUÍDO]")
    spark.stop()


if __name__ == "__main__":
    run_etl(use_sample_data=True)