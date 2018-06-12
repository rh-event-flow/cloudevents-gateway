package io.streamzi.strombrau.router.kafka;

import io.reactivex.Flowable;
import io.streamzi.strombrau.router.verticle.eb.EventFilterVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaInputConsumer extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(KafkaInputConsumer.class.getName());

    private EventBus eventBus;

    @Override
    public void start() throws Exception {
        eventBus = vertx.eventBus();
        ConfigRetriever retriever = ConfigRetriever.create(vertx);

        retriever.rxGetConfig().subscribe(myconf -> {

            final Map config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("KAFKA_SERVICE_HOST") + ":"  + myconf.getInteger("KAFKA_SERVICE_PORT").toString());
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonObjectDeserializer.class);

            Flowable<KafkaConsumerRecord<String, JsonObject>> stream = KafkaConsumer.<String, JsonObject>create(vertx, config)
                    .subscribe("gw.global.input.cloudevents")
                    .toFlowable();

            stream.subscribe(data -> {

                logger.fine("pumping data from Kafka to EB");
                // pump to EB:
                eventBus.publish(EventFilterVerticle.CE_ADDRESS, Json.encode(data.value()));
            });
        });
    }
}
