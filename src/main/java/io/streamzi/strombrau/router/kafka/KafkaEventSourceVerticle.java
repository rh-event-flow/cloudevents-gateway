package io.streamzi.strombrau.router.kafka;

import io.reactivex.Flowable;
import io.streamzi.strombrau.router.StrombrauBaseVerticle;
import io.streamzi.strombrau.router.verticle.eb.KafkaEventTopicPublisher;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.core.buffer.Buffer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaEventSourceVerticle extends StrombrauBaseVerticle {

    private static final Logger logger = Logger.getLogger(KafkaEventSourceVerticle.class.getName());

    @Override
    protected void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("Starting Kafka Ingest Verticle");


        retriever.rxGetConfig().subscribe(myconf -> {

            final Map config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_SERVICE_HOST") + ":"  + myconf.getInteger("MY_CLUSTER_KAFKA_SERVICE_PORT").toString());
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BufferDeserializer.class);

            Flowable<KafkaConsumerRecord<Buffer, Buffer>> stream = KafkaConsumer.<String, JsonObject>create(vertx, config)
                    .subscribe("gw.global.input.cloudevents")
                    .toFlowable();

            stream.subscribe(data -> {

                // pump to EB:
                logger.info("pumping raw data from Kafka to EB");
                try {
                    final JsonObject jsonObject = data.value().toJsonObject();
                    eventBus.publish(KafkaEventTopicPublisher.CE_ADDRESS, jsonObject);
                } catch (Exception e) {
                    logger.warning("Cloud not parse data: " + data.value());
                }
            });
        });
    }
}
