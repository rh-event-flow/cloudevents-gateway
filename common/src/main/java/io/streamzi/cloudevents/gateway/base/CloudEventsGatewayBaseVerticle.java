package io.streamzi.cloudevents.gateway.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vertx.core.json.Json;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.logging.Logger;

import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;

public abstract class CloudEventsGatewayBaseVerticle extends AbstractVerticle {

    static {

        // add Jackson datatype for ZonedDateTime
        Json.mapper.registerModule(new Jdk8Module());

        final SimpleModule module = new SimpleModule();
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        Json.mapper.registerModule(module);
    }


    private static final Logger logger = Logger.getLogger(CloudEventsGatewayBaseVerticle.class.getName());

    protected EventBus eventBus;

    public final void start() throws Exception {
        logger.fine("Starting base vertile");

        eventBus = vertx.eventBus();
        final ConfigRetriever retriever = ConfigRetriever.create(vertx);

        startStromBrauVerticle(retriever);

    }

    protected abstract void startStromBrauVerticle(ConfigRetriever retriever);

    private static class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
        @Override
        public void serialize(ZonedDateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(ISO_ZONED_DATE_TIME.format(value));
        }
    }
}
