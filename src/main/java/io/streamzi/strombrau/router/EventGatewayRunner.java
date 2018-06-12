package io.streamzi.strombrau.router;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.streamzi.strombrau.router.http.RxHttpServer;
import io.streamzi.strombrau.router.kafka.KafkaInputConsumer;
import io.streamzi.strombrau.router.verticle.eb.EventFilterVerticle;
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

        SimpleModule module = new SimpleModule();
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        Json.mapper.registerModule(module);
    }

    public static void main(String[] args) {
        logger.fine("Starting main() program");
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(EventGatewayRunner.class.getName());
    }

    @Override
    public void start() throws Exception {

        logger.info("Deploying verticals");
        vertx.deployVerticle(RxHttpServer.class.getName());
        vertx.deployVerticle(KafkaInputConsumer.class.getName());
        vertx.deployVerticle(EventFilterVerticle.class.getName());
    }

    private static class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
        @Override
        public void serialize(ZonedDateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(ISO_ZONED_DATE_TIME.format(value));
        }
    }

}
