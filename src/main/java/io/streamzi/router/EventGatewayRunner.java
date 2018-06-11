package io.streamzi.router;

import io.streamzi.router.http.RxHttpServer;
import io.streamzi.router.verticle.eb.EventFilterVerticle;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;

import java.util.logging.Logger;

public class EventGatewayRunner extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(EventGatewayRunner.class.getName());

    public static void main(String[] args) {
        logger.fine("Starting main() program");
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(EventGatewayRunner.class.getName());
    }

    @Override
    public void start() throws Exception {

        logger.info("Deploying verticals");
        vertx.deployVerticle(RxHttpServer.class.getName());
        vertx.deployVerticle(EventFilterVerticle.class.getName());
    }

}
