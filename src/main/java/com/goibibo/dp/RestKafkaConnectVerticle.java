package com.goibibo.dp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.shiro.ShiroAuth;
import io.vertx.ext.auth.shiro.ShiroAuthRealmType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Author : ShivamSharma
 * Time Created : 11/02/16.
 */
public class RestKafkaConnectVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(RestKafkaConnectVerticle.class);
    private Router router;
    private HttpServer httpServer;

    private KafkaProducer<String, String> producer;
    private Properties conf;

    RestKafkaConnectVerticle(KafkaProducer<String, String> producer, Properties conf) {
        this.producer = producer;
        this.conf = conf;
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        logger.info("Rest To Kafka Server started !!!");
        JsonObject config = new JsonObject().put("properties_path", "classpath:auth.properties");
        ShiroAuth shiroAuth = ShiroAuth.create(vertx, ShiroAuthRealmType.PROPERTIES, config);
        AuthHandler basicAuthHandler = BasicAuthHandler.create(shiroAuth);

        httpServer = vertx.createHttpServer();
        router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.route().handler(basicAuthHandler);

        router.route(HttpMethod.POST, "/resttokafka/:topic")
                .consumes("application/json")
                .produces("application/json")
                .handler(new RestHandler(producer, conf));

        httpServer.requestHandler(router::accept).listen(Integer.parseInt(conf.getProperty("port")), conf.getProperty("host"));
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        router.clear();
        logger.info("Rest To Kafka Server stopped!!!");
    }
}
