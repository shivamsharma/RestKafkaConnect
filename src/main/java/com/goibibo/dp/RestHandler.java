package com.goibibo.dp;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Project: vertx
 * Author: shivamsharma
 * Date: 6/19/17.
 */
public class RestHandler implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(RestHandler.class);

    private KafkaProducer<String, String> producer;
    private Properties conf;

    RestHandler(KafkaProducer<String, String> producer, Properties conf) {
        this.producer = producer;
        this.conf = conf;
    }

    @Override
    public void handle(RoutingContext routingContext) {
        logger.info("POST :: " + routingContext.currentRoute().getPath());
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json");

        HttpServerRequest request = routingContext.request();
        String topic = request.getParam("topic");

        if (routingContext.user() != null) {
            String username = routingContext.user().principal().getString("username");
            Stream<String> topics = null;
            if (conf.containsKey(username)) {
                topics = Arrays.stream(conf.getProperty(username).split(","))
                        .map(String::trim);
            }
            boolean isUserAuthorized = topics != null && topics.anyMatch(t -> t.equals(topic));

            if (isUserAuthorized) {
                JsonObject bodyAsJson = routingContext.getBodyAsJson();
                String key = "" + (int) (Math.random() * 10000);
                String message = bodyAsJson.toString();

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
                producer.send(record);
                response.setStatusCode(204).end();
            } else {
                logger.info("User " + username + " is not authorised to send data to kafka topic: " + topic);
                response.setStatusCode(401).end("User is not authorised to send data to kafka topic: " + topic);
            }
        } else {
            logger.info("Not a authenticated user");
            response.setStatusCode(401).end("Not a authenticated user");
        }
    }
}
