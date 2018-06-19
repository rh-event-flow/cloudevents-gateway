package io.streamzi.strombrau.router.verticle.eb;

import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import io.streamzi.strombrau.router.StrombrauBaseVerticle;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaEventTopicPublisher extends StrombrauBaseVerticle {

    private static final Logger logger = Logger.getLogger(KafkaEventTopicPublisher.class.getName());

    public static final String CE_ADDRESS = "couldEvent";

    private KafkaWriteStream<String, JsonObjectSerializer> writeStream;
    private KafkaProducer<String, String> producer;

    @Override
    public void startStromBrauVerticle(final ConfigRetriever retriever) {
        logger.info("Starting Event Router");

        retriever.rxGetConfig().subscribe(myconf -> {
            final Map config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myconf.getString("MY_CLUSTER_KAFKA_SERVICE_HOST") + ":"  + myconf.getInteger("MY_CLUSTER_KAFKA_SERVICE_PORT").toString());

            producer = KafkaProducer.create(vertx, config, String.class, String.class);
            writeStream = KafkaWriteStream.create(vertx.getDelegate(), config, String.class, String.class);
        });

        registerHandler();
    }

    private void registerHandler() {

                eventBus.consumer(CE_ADDRESS)
                .toFlowable()
                .subscribe(message -> {

                    CloudEvent ce = Json.decodeValue(message.body().toString(), CloudEventImpl.class);
                    logger.info("Publishing event to Kafka (" + ce.getEventType() +")");
                    writeStream.write(new ProducerRecord(ce.getEventType(), ce.getEventID(), message.body().toString()));
                });
    }
}
