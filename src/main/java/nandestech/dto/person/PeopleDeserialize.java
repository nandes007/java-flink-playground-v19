package nandestech.dto.person;

import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class PeopleDeserialize implements DeserializationSchema<PeopleOuterClass.People> {
    @Override
    public PeopleOuterClass.People deserialize(byte[] bytes) throws IOException {
        try {
            return PeopleOuterClass.People.parseFrom(bytes);
        } catch (Exception e) {
            System.out.println(e.toString());
            throw new IOException("Unable to deserialize bytes");
        }
    }

    @Override
    public boolean isEndOfStream(PeopleOuterClass.People people) {
        return false;
    }

    @Override
    public TypeInformation<PeopleOuterClass.People> getProducedType() {
        return TypeInformation.of(PeopleOuterClass.People.class);
    }
}
