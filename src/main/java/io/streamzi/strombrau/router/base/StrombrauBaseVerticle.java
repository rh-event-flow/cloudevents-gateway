package io.streamzi.strombrau.router.base;

import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;

import java.util.logging.Logger;

public abstract class StrombrauBaseVerticle extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(StrombrauBaseVerticle.class.getName());

    protected EventBus eventBus;

    public final void start() throws Exception {
        logger.fine("Starting base vertile");

        eventBus = vertx.eventBus();
        final ConfigRetriever retriever = ConfigRetriever.create(vertx);

        startStromBrauVerticle(retriever);

    }

    protected abstract void startStromBrauVerticle(ConfigRetriever retriever);
}
