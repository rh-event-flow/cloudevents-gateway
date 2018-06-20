package io.streamzi.strombrau.router;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.streamzi.strombrau.router.source.http.HttpEventSourceVerticle;
import io.streamzi.strombrau.router.source.kafka.KafkaEventSourceVerticle;
import io.streamzi.strombrau.router.sink.eventbus.KafkaEventTopicPublisher;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.logging.Logger;

import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;

public class EventGatewayRunner extends AbstractVerticle {

    private static final Logger logger = Logger.getLogger(EventGatewayRunner.class.getName());

    static {

        // add Jackson datatype for ZonedDateTime
        Json.mapper.registerModule(new Jdk8Module());

        final SimpleModule module = new SimpleModule();
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        Json.mapper.registerModule(module);
    }

    public static void main(String[] args) {
        logger.info("\uD83C\uDF7A \uD83C\uDF7A Starting Strombrau.io \uD83C\uDF7A \uD83C\uDF7A");
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(EventGatewayRunner.class.getName());
    }

    @Override
    public void start() throws Exception {

        logger.info("Deploying verticals");
        vertx.deployVerticle(KafkaEventTopicPublisher.class.getName());
        vertx.deployVerticle(HttpEventSourceVerticle.class.getName());
        vertx.deployVerticle(KafkaEventSourceVerticle.class.getName());
    }

    private static class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
        @Override
        public void serialize(ZonedDateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(ISO_ZONED_DATE_TIME.format(value));
        }
    }

}
