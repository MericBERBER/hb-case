package com.mericberber.hbcase.kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.handler.BigQueryHandler;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@EnableKafka
@Component
public class ProductViewKafkaConsumer {

    private final Gson gson = new GsonBuilder().create();
    @Autowired
    private BigQueryHandler bigQueryHandler;

    @KafkaListener(topics = KafkaConstants.ORDER_EVENT_TOPIC,
            containerFactory = "ProductViewContainerFactory")
    public void listenProductViewTopic(String productViewAsString) {

        ProductView productView = gson.fromJson(productViewAsString, ProductView.class);
        bigQueryHandler.handleProductView(productView);
    }
}
