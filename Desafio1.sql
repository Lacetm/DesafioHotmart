--Quais são os 50 maiores produtores em faturamento ($) de 2021?

SELECT
    p.producer_id,
    SUM(pi.purchase_value) AS fat
FROM purchase p
JOIN product_item pi ON p.prod_item_id = pi.prod_item_id
WHERE EXTRACT(YEAR FROM p.order_date) = 2021
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50

-- Quais são os 2 produtos que mais faturaram ($) de cada produtor?
WITH aux AS (
    SELECT
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value) AS fat,
        RANK() OVER (
            PARTITION BY p.producer_id
            ORDER BY SUM(pi.purchase_value) DESC
        ) AS ranking
    FROM purchase p
    JOIN product_item pi ON p.prod_item_id = pi.prod_item_id
    GROUP BY 1, 2
)

SELECT *
FROM aux
WHERE ranking <= 2