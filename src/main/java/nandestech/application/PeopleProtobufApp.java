package nandestech.application;

import nandestech.dto.PeopleCustomSerialization;
import nandestech.dto.PeopleProtobufDeserializationSchema;
import nandestech.protos.EventStoreOuterClass;
import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;
import com.twitter.chill.protobuf.ProtobufSerializer;

import java.util.Arrays;
import java.util.HashSet;

public class PeopleProtobufApp {
    public static final String TOPIC = "people_protobuf";
    public static final String BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().registerTypeWithKryoSerializer(
//                PeopleOuterClass.People.class,
//                new ProtobufSerializer()
////                ProtobufSerializer.class
//        );
        env.getConfig().registerTypeWithKryoSerializer(
                PeopleOuterClass.People.class,
                ProtobufSerializer.class
        );
//        env.getConfig().disableGenericTypes();

         final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
                new TopicPartition("input", 0),
                new TopicPartition("input", 1),
                new TopicPartition("input", 2)
        ));

        KafkaSource<PeopleOuterClass.People> source =
                KafkaSource.<PeopleOuterClass.People>builder()
                        .setBootstrapServers(BROKERS)
                        .setProperty("partition.discovery.interval.ms", "10000")
                        .setTopics(TOPIC)
                        .setGroupId("groupId-protobuf-01")
                        .setStartingOffsets(OffsetsInitializer.earliest())
//                        .setValueOnlyDeserializer(new PeopleProtobufDeserializationSchema())
                        .setValueOnlyDeserializer(new PeopleProtobufDeserializationSchema())
                        .build();

        DataStreamSource<PeopleOuterClass.People> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka");
//        System.out.println("Running Debugging...");

        kafka.print();

        kafka.addSink(JdbcSink.sink("insert into public.people (name, timestamp, country, job, image) values (?, ?, ?, ?, ?)",
                (statement, event) -> {
                    statement.setString(1, event.getName());
                    statement.setString(2, event.getTimestamp());
                    statement.setString(3, event.getCountry());
                    statement.setString(4, event.getJob());
                    statement.setString(5, event.getImage());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://172.27.216.159:5432/core_local")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("nandes")
                        .withPassword("postgre")
                        .build()
        ));

        env.execute("Kafka2postgres");
    }
}
