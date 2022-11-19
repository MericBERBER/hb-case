package com.mericberber.hbcase.rest.constants;

public class BigQueryConstants {

    public static final String GET_MOST_VIEWED_PRODUCT_AT_LAST_HOURS_SQL =
            "SELECT pv.properties.productid as productid, COUNT(*) as view_count FROM `hbcase.product-views` as pv\n" +
            "WHERE pv.timestamp > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -60 MINUTE)\n" +
            "GROUP BY pv.properties.productid \n" +
            "ORDER BY view_count DESC\n" +
            "LIMIT 100;";
    public static final String GET_MOST_VIEWED_5_PRODUCT_BY_USER = "SELECT pv.userid, pv.properties.productid, COUNT(*) as view_count \n" +
            "FROM `hbcase.product-views` as pv\n" +
            "WHERE pv.userid = '%s'\n" +
            "GROUP BY pv.userid, pv.properties.productid\n" +
            "ORDER BY view_count DESC\n" +
            "LIMIT 5;";
}
