from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow import DAG
from datetime import datetime, timedelta



default_args = {
    'start_date' : datetime(2022,11,11)
}

dag = DAG('hbcase_flow', schedule_interval=None, default_args=default_args)


find_top_10_most_bought = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="find_top_10_most_bought",
    sql="""
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
    """,
    destination_dataset_table=f"hbcase.top_ten_most_bought",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="my_bigquery_connection",
    use_legacy_sql=False,
)


find_top_10_most_viewed = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="find_top_10_most_viewed",
    sql="""
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
    """,
    destination_dataset_table=f"hbcase.top_ten_most_viewed",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="my_bigquery_connection",
    use_legacy_sql=False,
)

conversion_rate = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="conversion_rate",
    sql="""
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
    """,
    destination_dataset_table=f"hbcase.conversion_rate",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="my_bigquery_connection",
    use_legacy_sql=False,
)