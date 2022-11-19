SELECT  * FROM(
    SELECT *,
    RANK() OVER(PARTITION BY T.cid ORDER BY T.distinct_user_count DESC) as rn
    FROM(
      SELECT pc.categoryid AS cid, pc.productid as pid, COUNT(DISTINCT userid) as distinct_user_count
      FROM `hbcase.product-views` as pv
      JOIN `hbcase.product-category` as pc
      ON pv.properties.productid = pc.productid
      GROUP BY pc.categoryid, pc.productid) as T ) as T2
WHERE t2.rn <= 10
ORDER BY T2.cid, T2.pid;
