package io.streamzi.strombrau.router.http;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.streamzi.strombrau.router.StrombrauBaseVerticle;
import io.streamzi.strombrau.router.verticle.eb.KafkaEventTopicPublisher;
import io.vertx.core.json.Json;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.core.http.HttpServerRequest;

import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.streamzi.cloudevents.CloudEvent;

import java.util.logging.Logger;

public class HttpEventSourceVerticle extends StrombrauBaseVerticle {

    private static final Logger logger = Logger.getLogger(HttpEventSourceVerticle.class.getName());

    @Override
    public void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("Starting HTTP Ingest Verticle");

        final HttpServer server = vertx.createHttpServer();
        final Flowable<HttpServerRequest> requestFlowable = server.requestStream().toFlowable();

        requestFlowable.subscribe(httpServerRequest -> {

            final Observable<CloudEvent> observable = httpServerRequest
                    .toObservable()
                    .compose(io.vertx.reactivex.core.ObservableHelper.unmarshaller(CloudEventImpl.class));

            observable.subscribe(cloudEvent -> {

                if (httpServerRequest.path().equals("/ce")) {
                    logger.fine("Received Event-Type: " + cloudEvent.getEventType());

                    // todo: proper encoding
                    // ship it!
                    eventBus.publish(KafkaEventTopicPublisher.CE_ADDRESS, Json.encode(cloudEvent));


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
