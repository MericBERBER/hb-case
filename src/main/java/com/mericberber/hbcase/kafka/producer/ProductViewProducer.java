package com.mericberber.hbcase.kafka.producer;

import com.google.gson.Gson;
import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import org.apache.kafka.clients.producer.ProducerRecord;
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
public class ProductViewProducer {

    @Qualifier("ProductViewKafkaTemplate")
    @Autowired
    private KafkaTemplate<String, ProductView> kafkaTemplate;
    private final Gson gson = new Gson();

    public void produceEvents() {

        List<ProductView> productViews = null;
        try {
            productViews = Files.readAllLines(Paths.get("src/main/resources/data/product-views.json"))
                    .stream()
                    .map(line -> gson.fromJson(line, ProductView.class))
                    .collect(Collectors.toList());

            for (ProductView productView : productViews) {
                productView.setTimestamp(new Timestamp(System.currentTimeMillis()));
                kafkaTemplate.send(new ProducerRecord<>(KafkaConstants.PRODUCT_VIEW_TOPIC, productView.getUserId(), productView));
                System.out.println(productView);
                Thread.sleep(1000);

            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


    }
}
