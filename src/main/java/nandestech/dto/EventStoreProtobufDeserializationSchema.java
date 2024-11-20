package nandestech.dto;

import nandestech.protos.EventStoreOuterClass;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class EventStoreProtobufDeserializationSchema extends AbstractDeserializationSchema<EventStoreOuterClass.EventStore> {

    @Override
    public EventStoreOuterClass.EventStore deserialize(byte[] bytes) throws IOException {
        return EventStoreOuterClass.EventStore.parseFrom(bytes);
    }
}
