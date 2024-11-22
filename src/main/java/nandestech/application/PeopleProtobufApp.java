package nandestech.application;

import nandestech.dto.PeopleCustomSerialization;
import nandestech.dto.PeopleProtobufDeserializationSchema;
import nandestech.dto.person.GenericBinaryProtoDeserializer;
import nandestech.dto.person.PeopleDeserialize;
import nandestech.protos.EventStoreOuterClass;
import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import com.twitter.chill.protobuf.ProtobufSerializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

public class PeopleProtobufApp {
    public static final String TOPIC = "people_protobuf";
    public static final String BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("client.id", "PeopleConsumerClient");
        consumerProperties.setProperty("offset.reset", "earliest");

        GenericBinaryProtoDeserializer<PeopleOuterClass.People> genericDeserializer = new GenericBinaryProtoDeserializer<>(PeopleOuterClass.People.class);
        PeopleDeserialize specificDeserializer = new PeopleDeserialize();

        FlinkKafkaConsumer<PeopleOuterClass.People> personsKafkaConsumer =
                new FlinkKafkaConsumer<PeopleOuterClass.People>(TOPIC, genericDeserializer, consumerProperties);

        DataStream<PeopleOuterClass.People> personStreamOut = env.addSource(personsKafkaConsumer);
//        DataStream<PeopleOuterClass.People> adultPersonStream = personStreamOut.filter(person -> person.getAge() >= 18);
        DataStream<String> result = personStreamOut.map(new MapFunction<PeopleOuterClass.People, String>() {
            @Override
            public String map(PeopleOuterClass.People people) {
                return String.format("The Person %s is adult (job %s)", people.getName(), people.getJob());
            }
        });

        result.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}
