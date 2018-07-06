package io.streamzi.router.sink.eventbus;

import io.reactivex.Flowable;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.streamzi.router.base.StrombrauBaseVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class HttpEventPublisher extends StrombrauBaseVerticle {

    private static final Logger logger = Logger.getLogger(HttpEventPublisher.class.getName());

    public static final String CE_ADDRESS = "couldEvent";

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
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "io.streamzi.kafka.cloudevent.type." + myconf.getString("STREAMZI_EVENT_TYPE"));
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);


        Flowable<KafkaConsumerRecord<Buffer, Buffer>> stream = KafkaConsumer.<String, JsonObject>create(vertx, consumerConfig)
                .subscribe("gw.global.input.cloudevents")
                .toFlowable();

        stream.subscribe(data -> {

            final JsonObject jsonObject = data.value().toJsonObject();
            logger.info("Publishing event to HTTP: (" + jsonObject.getString("eventType") + ")");

            client.post(myconf.getString("HTTP_OUTPUT_ENDPOINT"), response -> {
                System.out.println("Received response with status code " + response.statusCode());
            }).putHeader("content-type", "text/plain").end(jsonObject.toString());
        });
    }
}
