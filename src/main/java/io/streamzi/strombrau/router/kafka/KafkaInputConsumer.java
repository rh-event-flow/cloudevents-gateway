package io.streamzi.strombrau.router.kafka;

import io.reactivex.Flowable;
import io.streamzi.strombrau.router.verticle.eb.EventFilterVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.BufferDeserializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaInputConsumer extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(KafkaInputConsumer.class.getName());

    private EventBus eventBus;

    @Override
    public void start() {
        logger.info("Starting generic Kafka consumer");

        eventBus = vertx.eventBus();
        ConfigRetriever retriever = ConfigRetriever.create(vertx);

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

                logger.info("pumping raw data from Kafka to EB");

                // pump to EB:
                eventBus.publish(EventFilterVerticle.CE_ADDRESS, data.value().toJsonObject());
            });
        });
    }
}
