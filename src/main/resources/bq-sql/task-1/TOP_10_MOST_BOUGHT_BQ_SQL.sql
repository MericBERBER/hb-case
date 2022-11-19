SELECT * FROM(
  SELECT *,
  RANK() OVER(PARTITION BY categoryid ORDER BY distinct_user_count DESC) as rn
  FROM(
    SELECT pc.categoryid, pc.productid, COUNT(DISTINCT T.userid) as distinct_user_count FROM
    `hbcase.product-category` as pc
  JOIN (
    SELECT userid,items FROM `hbcase.orders` as orders
    CROSS JOIN UNNEST(orders.lineitems) as items ) as T
  ON T.items.productid = pc.productid
  GROUP BY pc.categoryid, pc.productid
  ) as T2
)
WHERE rn <= 10
ORDER BY categoryid, productid;

