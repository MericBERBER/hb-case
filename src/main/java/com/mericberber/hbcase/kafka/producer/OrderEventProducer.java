package com.mericberber.hbcase.kafka.producer;

import com.google.gson.Gson;
import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class OrderEventProducer {

    @Qualifier("OrderEventKafkaTemplate")
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private final Gson gson = new Gson();

    public void produceEvents() {

        List<OrderEvent> orderEvents = null;
        try {
            orderEvents = Files.readAllLines(Paths.get("src/main/resources/data/orders.json"))
                    .stream()
                    .map(line -> gson.fromJson(line, OrderEvent.class))
                    .collect(Collectors.toList());

            for (OrderEvent orderEvent : orderEvents) {
                orderEvent.setTimestamp(new Timestamp(System.currentTimeMillis()));
                kafkaTemplate.send(new ProducerRecord<>(KafkaConstants.ORDER_EVENT_TOPIC, orderEvent.getUserId(), orderEvent));
                System.out.println(orderEvent);
                Thread.sleep(60000);

            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
