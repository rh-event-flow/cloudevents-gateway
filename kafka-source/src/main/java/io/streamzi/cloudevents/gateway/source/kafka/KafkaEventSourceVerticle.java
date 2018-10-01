package io.streamzi.cloudevents.gateway.source.kafka;

import io.reactivex.Flowable;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.streamzi.cloudevents.gateway.base.CloudEventsGatewayBaseVerticle;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaEventSourceVerticle extends CloudEventsGatewayBaseVerticle {

    private KafkaWriteStream<String, JsonObjectSerializer> writeStream;


    private static final Logger logger = Logger.getLogger(KafkaEventSourceVerticle.class.getName());

    @Override
    protected void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("\uD83C\uDF7A \uD83C\uDF7A Starting Kafka Ingest Verticle");

        retriever.rxGetConfig().subscribe(myconf -> {

            final Map consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT_CLIENTS") + ":" + myconf.getInteger("MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT").toString());
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "io.streamzi.kafka.source");
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);

            final Map producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT_CLIENTS") + ":" + myconf.getInteger("MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT").toString());
            writeStream = KafkaWriteStream.create(vertx.getDelegate(), producerConfig, String.class, String.class);


            Flowable<KafkaConsumerRecord<Buffer, Buffer>> stream = KafkaConsumer.<String, JsonObject>create(vertx, consumerConfig)
                    .subscribe("gw.global.input.cloudevents")
                    .toFlowable();

            stream.subscribe(data -> {

                logger.info("Received Event-Type on Kafka endpoint");
                try {
                    final JsonObject jsonObject = data.value().toJsonObject();

                    CloudEvent ce = Json.decodeValue(jsonObject.toString(), CloudEventImpl.class);
                    writeStream.write(new ProducerRecord(ce.getEventType(), ce.getEventID(), Json.encode(ce)));

                } catch (Exception e) {
                    logger.warning("Cloud not parse data: " + data.value());
                }
            });
        });
    }
}
