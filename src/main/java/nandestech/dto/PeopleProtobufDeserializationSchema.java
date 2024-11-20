package nandestech.dto;

import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class PeopleProtobufDeserializationSchema implements DeserializationSchema<PeopleOuterClass.People> {

    @Override
    public PeopleOuterClass.People deserialize(byte[] message) throws IOException {
        return PeopleOuterClass.People.parseFrom(message);
    }

    @Override
    public boolean isEndOfStream(PeopleOuterClass.People people) {
        return false;
    }

    @Override
    public TypeInformation<PeopleOuterClass.People> getProducedType() {
        return TypeInformation.of(PeopleOuterClass.People.class);
//        return null;
    }
}
