package io.streamzi.cloudevents.gateway.sink.eventbus;

import io.reactivex.Flowable;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import io.streamzi.cloudevents.gateway.base.CloudEventsGatewayBaseVerticle;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class HttpEventPublisher extends CloudEventsGatewayBaseVerticle {

    private static final Logger logger = Logger.getLogger(HttpEventPublisher.class.getName());
    private HttpClient client;

    @Override
    public void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("Kafka to HTTP Bridge");
        client = vertx.createHttpClient();
        retriever.rxGetConfig().subscribe(myconf -> registerHandler(myconf));
    }

    private void registerHandler(final JsonObject myconf) {


        final Map consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_SERVICE_HOST") + ":" + myconf.getInteger("MY_CLUSTER_KAFKA_SERVICE_PORT").toString());

        // if STREAMZI_GROUP is not defined, we use a UUID, to guarantee unique consumer group, per EVENT_TYPE
        final String streamziGroup = myconf.getString("STREAMZI_GROUP") != null ? myconf.getString("STREAMZI_GROUP") : UUID.randomUUID().toString();
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "io.streamzi.kafka.cloudevent." + myconf.getString("STREAMZI_EVENT_TYPE") + "." + streamziGroup);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);


        Flowable<KafkaConsumerRecord<Buffer, Buffer>> stream = KafkaConsumer.<String, JsonObject>create(vertx, consumerConfig)
                .subscribe(myconf.getString("STREAMZI_EVENT_TYPE"))
                .toFlowable();

        stream.subscribe(data -> {

            final JsonObject jsonObject = data.value().toJsonObject();
            logger.info("Publishing event to HTTP: (" + jsonObject.getString("eventType") + ")");
            logger.info("Posting to ->   " + myconf.getString("HTTP_OUTPUT_ENDPOINT"));

            client.postAbs(myconf.getString("HTTP_OUTPUT_ENDPOINT"), response -> {
                System.out.println("Received response with status code " + response.statusCode());
            }).putHeader("content-type", "text/plain").end(jsonObject.toString());
        });
    }
}
