package nandestech.dto;

import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class PeopleCustomSerialization extends TypeSerializer<PeopleOuterClass.People> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<PeopleOuterClass.People> duplicate() {
        return this;
    }

    @Override
    public PeopleOuterClass.People createInstance() {
        return PeopleOuterClass.People.getDefaultInstance();
    }

    @Override
    public PeopleOuterClass.People copy(PeopleOuterClass.People from) {
        return from.toBuilder().build();
    }

    @Override
    public PeopleOuterClass.People copy(PeopleOuterClass.People people, PeopleOuterClass.People t1) {
        return copy(people);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(PeopleOuterClass.People record, DataOutputView target) throws IOException {
        byte[] data = record.toByteArray();
        target.writeInt(data.length);
        target.write(data);
    }

    @Override
    public PeopleOuterClass.People deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] data = new byte[length];
        source.readFully(data);
        return PeopleOuterClass.People.parseFrom(data);
    }

    @Override
    public PeopleOuterClass.People deserialize(PeopleOuterClass.People people, DataInputView dataInputView) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {

    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<PeopleOuterClass.People> snapshotConfiguration() {
        return null;
    }
}
