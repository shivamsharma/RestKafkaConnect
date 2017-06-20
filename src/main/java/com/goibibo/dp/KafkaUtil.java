package com.goibibo.dp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Project: vertx
 * Author: shivamsharma
 * Date: 6/19/17.
 */
class KafkaUtil {

    static KafkaProducer<String, String> getProducer(String bootstrapServers) {
        String hostName = "";
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        props.put("client.id", hostName);
        props.put("acks", "all");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
}
