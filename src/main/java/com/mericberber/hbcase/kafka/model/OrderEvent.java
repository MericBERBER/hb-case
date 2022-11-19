package com.mericberber.hbcase.kafka.model;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.util.List;

@Getter
@Setter
public class OrderEvent {

    private String event;
    @SerializedName("messageid")
    private String messageId;
    @SerializedName("userid")
    private String userId;
    @SerializedName("lineitems")
    private List<LineItem> lineItems;
    @SerializedName("orderid")
    private int orderId;
    private Timestamp timestamp;

    @Getter
    @Setter
    public static class LineItem {
        @SerializedName("productid")
        private String productId;
        private int quantity;
    }
}



