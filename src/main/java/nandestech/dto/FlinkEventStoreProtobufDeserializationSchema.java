package nandestech.dto;

import nandestech.protos.EventStoreOuterClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class FlinkEventStoreProtobufDeserializationSchema implements DeserializationSchema<EventStoreOuterClass.EventStore> {

    @Override
    public EventStoreOuterClass.EventStore deserialize(byte[] bytes) throws IOException {
        return EventStoreOuterClass.EventStore.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(EventStoreOuterClass.EventStore eventStore) {
        return false;
    }

    @Override
    public TypeInformation<EventStoreOuterClass.EventStore> getProducedType() {
        return TypeInformation.of(EventStoreOuterClass.EventStore.class);
//        return null;
    }
}
