package com.mericberber.hbcase.kafka.handler;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.*;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class BigQueryHandler {

    @Autowired
    BigQuery bigquery;
    private final String datasetName = "hbcase";
    private final String orderTable = "orders";
    private final String productViewTable = "product-views";

    public void handleOrderEvent(OrderEvent orderEvent) {

        // Create a row to insert
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("orderid", orderEvent.getOrderId());
        rowContent.put("userid", orderEvent.getUserId());
        rowContent.put("messageid", orderEvent.getMessageId());
        rowContent.put("event", orderEvent.getEvent());
        rowContent.put("timestamp", orderEvent.getTimestamp().toString());
        rowContent.put("lineitems", orderEvent.getLineItems());

        tableInsertRows(datasetName, orderTable, rowContent);

    }

    public void handleProductView(ProductView productView) {

        // Create a row to insert
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("context", productView.getContext());
        rowContent.put("properties", productView.getProperties());
        rowContent.put("userid", productView.getUserId());
        rowContent.put("messageid", productView.getMessageId());
        rowContent.put("event", productView.getEvent());
        rowContent.put("timestamp", productView.getTimestamp().toString());

        tableInsertRows(datasetName, orderTable, rowContent);
    }

    private void tableInsertRows(
            String datasetName, String tableName, Map<String, Object> rowContent) {
        try {

            TableId tableId = TableId.of(datasetName, tableName);

            InsertAllResponse response =
                    bigquery.insertAll(
                            InsertAllRequest.newBuilder(tableId)
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
