
    SELECT order_count_table.categoryid as category, order_count, view_count, (order_count / view_count) as conversion_rate FROM
    (
    SELECT pc.categoryid, COUNT(*) as order_count FROM
    `hbcase.product-category` as pc
    JOIN
    (SELECT items
    FROM `hbcase.orders` as o
    CROSS JOIN UNNEST(o.lineitems) as items ) as orders
    ON orders.items.productid = pc.productid
    GROUP BY pc.categoryid
    ) as order_count_table
    JOIN
    (
    SELECT pc.categoryid, COUNT(*) as view_count  FROM
    `hbcase.product-category` as pc
    JOIN  `hbcase.product-views`as pv
    ON pc.productid = pv.properties.productid
    GROUP BY pc.categoryid
    ) as view_count_table
    ON order_count_table.categoryid = view_count_table.categoryid
