package io.streamzi.strombrau.router.http;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.streamzi.strombrau.router.verticle.eb.EventFilterVerticle;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.core.http.HttpServerRequest;

import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.streamzi.cloudevents.CloudEvent;

import java.util.logging.Logger;

public class RxHttpServer extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(RxHttpServer.class.getName());

    private EventBus eventBus;

    @Override
    public void start() throws Exception {

        logger.info("Start of /ce HTTP endpoint");
        eventBus = vertx.eventBus();

        final HttpServer server = vertx.createHttpServer();
        final Flowable<HttpServerRequest> requestFlowable = server.requestStream().toFlowable();

        requestFlowable.subscribe(httpServerRequest -> {

            final Observable<CloudEvent> observable = httpServerRequest
                    .toObservable()
                    .compose(io.vertx.reactivex.core.ObservableHelper.unmarshaller(CloudEventImpl.class));

            observable.subscribe(cloudEvent -> {

                if (httpServerRequest.path().equalsIgnoreCase("/ce")) {
                    logger.fine("Received Event-Type: " + cloudEvent.getEventType());

                    // todo: proper encoding
                    // ship it!
                    eventBus.publish(EventFilterVerticle.CE_ADDRESS, Json.encode(cloudEvent));


                } else {
                    logger.fine("Ignoring request");
                }
            });

            // finish the incoming request, with a ACCEPT response...
            httpServerRequest.response().setChunked(true)
                    .putHeader("content-type", "text/plain")
                    .setStatusCode(201) // accepted
                    .end("Event received");
        });

        server.listen(8081);
    }
}
