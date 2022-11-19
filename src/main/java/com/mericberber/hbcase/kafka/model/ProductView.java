package com.mericberber.hbcase.kafka.model;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
public class ProductView {

    private String event;
    @SerializedName("messageid")
    private String messageId;
    @SerializedName("userid")
    private String userId;
    private Properties properties;
    private Context context;
    private Timestamp timestamp;
    @Getter
    @Setter
    public static class Context{
        private String source;
    }

    @Getter
    @Setter
    public static class Properties{
        @SerializedName("productid")
        private String productId;
    }
}
