package com.mericberber.hbcase.kafka.serdes;


import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {

    public static Serde<OrderEvent> OrderEvent() {
        JsonSerializer<OrderEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<ProductView> ProductView() {
        JsonSerializer<ProductView> serializer = new JsonSerializer<>();
        JsonDeserializer<ProductView> deserializer = new JsonDeserializer<>(ProductView.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}