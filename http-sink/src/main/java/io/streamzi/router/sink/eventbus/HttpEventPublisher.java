package io.streamzi.router.sink.eventbus;

import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.streamzi.router.base.StrombrauBaseVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.http.HttpClient;

import java.util.logging.Logger;

public class HttpEventPublisher extends StrombrauBaseVerticle {

    private static final Logger logger = Logger.getLogger(HttpEventPublisher.class.getName());

    public static final String CE_ADDRESS = "couldEvent";

    private HttpClient client;

    @Override
    public void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("Starting Event Router");

        client = vertx.createHttpClient();

        retriever.rxGetConfig().subscribe(myconf -> registerHandler(myconf));
    }

    private void registerHandler(final JsonObject myconf) {

                eventBus.consumer(CE_ADDRESS)
                .toFlowable()
                .subscribe(message -> {

                    final CloudEvent ce = Json.decodeValue(message.body().toString(), CloudEventImpl.class);
                    logger.info("Publishing event to HTTP (" + ce.getEventType() +")");

                    client.post(myconf.getString("HTTP_OUTPUT_ENDPOINT"), response -> {
                        System.out.println("Received response with status code " + response.statusCode());
                    }).putHeader("content-type", "text/plain").end(message.body().toString());
                });
    }
}
