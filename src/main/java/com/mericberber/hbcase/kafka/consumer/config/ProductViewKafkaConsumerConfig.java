package com.mericberber.hbcase.kafka.consumer.config;

import com.mericberber.hbcase.kafka.constants.KafkaConstants;
import com.mericberber.hbcase.kafka.model.OrderEvent;
import com.mericberber.hbcase.kafka.model.ProductView;
import com.mericberber.hbcase.kafka.serdes.JsonDeserializer;
import com.mericberber.hbcase.kafka.serdes.JsonSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class ProductViewKafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, ProductView> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "ProductViewContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, ProductView> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, ProductView> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
