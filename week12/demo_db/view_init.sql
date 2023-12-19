CREATE VIEW gmv as (
    SELECT
      pu.store_id,
      pr.category_id,
      SUM(pui.product_count * pui.product_price) AS sales_sum
    FROM purchases pu
    JOIN purchase_items pui
      ON pu.purchase_id = pui.purchase_id
    JOIN products pr
      ON pui.product_id = pr.product_id
    GROUP BY
      pu.store_id,
      pr.category_id
  );