package com.mericberber.hbcase.kafka.producer.config;

import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import com.mericberber.hbcase.kafka.serdes.JsonSerdes;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProductViewProducerConfig {

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerdes.ProductView().serializer().getClass());
        return props;
    }

    @Bean
    public ProducerFactory<String, ProductView> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean(name = "ProductViewKafkaTemplate")
    public KafkaTemplate<String, ProductView> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
