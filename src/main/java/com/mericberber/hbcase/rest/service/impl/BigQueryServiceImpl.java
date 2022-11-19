package com.mericberber.hbcase.rest.service.impl;

import com.google.cloud.bigquery.*;
import com.mericberber.hbcase.rest.constants.BigQueryConstants;
import com.mericberber.hbcase.rest.model.ProductViewCount;
import com.mericberber.hbcase.rest.model.ProductViewedByUser;
import com.mericberber.hbcase.rest.service.BigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BigQueryServiceImpl implements BigQueryService {

    @Autowired
    BigQuery bigquery;

    @Override
    public List<ProductViewCount> getMostViewedProductsInLastHour() throws InterruptedException {

        List<ProductViewCount> productViewCounts = new ArrayList<>();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(BigQueryConstants.GET_MOST_VIEWED_PRODUCT_AT_LAST_HOURS_SQL).build();

        // Run the query using the BigQuery object
        for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
            ProductViewCount productViewCount = new ProductViewCount();
            productViewCount.setProductId(row.get("productid").getStringValue());
            productViewCount.setViewCount(row.get("view_count").getLongValue());
            productViewCounts.add(productViewCount);
        }

        return productViewCounts;
    }

    @Override
    public ProductViewedByUser getLastFiveProductsViewedByUser(String userid) throws InterruptedException {

        ProductViewedByUser productViewedByUser = new ProductViewedByUser();
        productViewedByUser.setUserId(userid);
        String query = String.format(BigQueryConstants.GET_MOST_VIEWED_5_PRODUCT_BY_USER, userid);

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();


        List<ProductViewedByUser.ViewedProduct> products = new ArrayList<>();
        for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {


            ProductViewedByUser.ViewedProduct viewedProduct = new ProductViewedByUser.ViewedProduct();
            viewedProduct.setProductId(row.get("productid").getStringValue());
            viewedProduct.setViewCount(row.get("view_count").getLongValue());
            products.add(viewedProduct);
        }

        productViewedByUser.setProducts(products);
        return productViewedByUser;

    }

    @Override
    public void insertTest() {

        String datasetName = "hbcase";
        String tableName = "orders";
        // Create a row to insert
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("orderid", "123123");
        rowContent.put("userid", "123123");

        tableInsertRows(datasetName, tableName, rowContent);
    }

    public  void tableInsertRows(
            String datasetName, String tableName, Map<String, Object> rowContent) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.

            // Get table
            TableId tableId = TableId.of(datasetName, tableName);

            // Inserts rowContent into datasetName:tableId.
            InsertAllResponse response =
                    bigquery.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    // More rows can be added in the same RPC by invoking .addRow() on the builder.
                                    // You can also supply optional unique row keys to support de-duplication
                                    // scenarios.
                                    .addRow(rowContent)
                                    .build());

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println("Response error: \n" + entry.getValue());
                }
            }
            System.out.println("Rows successfully inserted into table");
        } catch (BigQueryException e) {
            System.out.println("Insert operation not performed \n" + e.toString());
        }
    }
}
