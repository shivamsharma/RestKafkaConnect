package com.goibibo.dp;

import io.vertx.core.Vertx;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author : ShivamSharma
 * Time Created : 11/02/16.
 */
public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        Vertx vertx = Vertx.vertx();

        InputStream restKafkaConfStream = Main.class.getClassLoader().getResourceAsStream("rest-to-kafka.conf");
        Properties conf = new Properties();
        conf.load(restKafkaConfStream);

        KafkaProducer<String, String> producer = KafkaUtil.getProducer(conf.getProperty("brokers"));

        vertx.deployVerticle(new RestKafkaConnectVerticle(producer, conf));
    }
}
