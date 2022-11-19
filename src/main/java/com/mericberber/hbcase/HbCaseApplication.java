package com.mericberber.hbcase;

import com.mericberber.hbcase.kafka.producer.OrderEventProducer;
import com.mericberber.hbcase.kafka.producer.ProductViewProducer;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;


@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
public class HbCaseApplication implements CommandLineRunner {

    @Autowired
    OrderEventProducer orderEventProducer;
    @Autowired
    ProductViewProducer productViewProducer;

    public static void main(String[] args) {
        SpringApplication.run(HbCaseApplication.class, args);
    }

    @Override
    public void run(String... args) {

        Thread productViewThread = new Thread(() -> productViewProducer.produceEvents());
        Thread orderEventThread = new Thread(() -> orderEventProducer.produceEvents());

        productViewThread.start();
        orderEventThread.start();

    }
}
