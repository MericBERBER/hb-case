SELECT pv.userid, pv.properties.productid, COUNT(*) as view_count
FROM `hbcase.product-views` as pv
WHERE pv.userid = 'user-1'
GROUP BY pv.userid, pv.properties.productid
ORDER BY view_count DESC
LIMIT 5;