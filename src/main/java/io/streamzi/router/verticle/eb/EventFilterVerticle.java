package io.streamzi.router.verticle.eb;

import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;

import java.util.logging.Logger;

public class EventFilterVerticle extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(EventFilterVerticle.class.getName());

    public static final String CE_ADDRESS = "couldEvent";
    private EventBus eventBus;

    @Override
    public void start() throws Exception {
        logger.info("Starting EventBus filter/consumer");
        assignEventBus();
        registerHandler();
    }

    private void assignEventBus() {
        eventBus = vertx.eventBus();
    }

    private void registerHandler() {

                eventBus.consumer(CE_ADDRESS)
                .toFlowable()
                .subscribe(message -> {

                    logger.info("Reveived from EB: " + message.body());
                    // todo: put to Kafka!
                });
    }
}
