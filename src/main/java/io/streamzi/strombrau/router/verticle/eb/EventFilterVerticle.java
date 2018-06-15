package io.streamzi.strombrau.router.verticle.eb;

import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class EventFilterVerticle extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(EventFilterVerticle.class.getName());

    public static final String CE_ADDRESS = "couldEvent";
    private EventBus eventBus;
    private KafkaWriteStream<String, JsonObjectSerializer> writeStream;
    private KafkaProducer<String, String> producer;

    @Override
    public void start() throws Exception {
        logger.info("Starting EventBus filter/consumer");

        ConfigRetriever retriever = ConfigRetriever.create(vertx);

        retriever.rxGetConfig().subscribe(myconf -> {
            final Map config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_SERVICE_HOST") + ":"  + myconf.getInteger("MY_CLUSTER_KAFKA_SERVICE_PORT").toString());

            producer = KafkaProducer.create(vertx, config, String.class, String.class);
            writeStream = KafkaWriteStream.create(vertx.getDelegate(), config, String.class, String.class);
        });

        assignEventBus();
        registerHandler();

    }

    private void assignEventBus() {
        eventBus = vertx.eventBus();
    }

    private void registerHandler() {

                eventBus.consumer(CE_ADDRESS)
                .toFlowable()
                .subscribe(message -> {

                    CloudEvent ce = Json.decodeValue(message.body().toString(), CloudEventImpl.class);
                    logger.info("Reveived on EB: " + ce.getEventType());
                    writeStream.write(new ProducerRecord(ce.getEventType(), ce.getEventID(), message.body().toString()));
                });
    }
}
