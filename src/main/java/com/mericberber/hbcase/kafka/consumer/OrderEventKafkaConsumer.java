package com.mericberber.hbcase.kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.handler.BigQueryHandler;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import org.checkerframework.checker.units.qual.C;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@EnableKafka
public class OrderEventKafkaConsumer {

    private final Gson gson = new GsonBuilder().create();
    @Autowired
    BigQueryHandler bigQueryHandler;

    @KafkaListener(topics = KafkaConstants.ORDER_EVENT_TOPIC,
            containerFactory = "OrderEventContainerFactory")
    public void listenOrderTopic(String orderEventAsString) {
        OrderEvent orderEvent = gson.fromJson(orderEventAsString, OrderEvent.class);
        bigQueryHandler.handleOrderEvent(orderEvent);
    }
}
