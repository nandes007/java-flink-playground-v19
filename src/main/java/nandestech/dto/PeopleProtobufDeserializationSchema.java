package nandestech.dto;

import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class PeopleProtobufDeserializationSchema extends AbstractDeserializationSchema<PeopleOuterClass.People> {

    @Override
    public PeopleOuterClass.People deserialize(byte[] message) throws IOException {
        return PeopleOuterClass.People.parseFrom(message);
    }
}
