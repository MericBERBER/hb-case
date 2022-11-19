SELECT pv.properties.productid as productid, COUNT(*) as view_count FROM `hbcase.product-views` as pv
WHERE pv.timestamp > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -60 MINUTE)
GROUP BY pv.properties.productid
ORDER BY view_count DESC
LIMIT 100;