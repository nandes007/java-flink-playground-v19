package nandestech.application;

import com.twitter.chill.protobuf.ProtobufSerializer;
import nandestech.dto.FlinkEventStoreProtobufDeserializationSchema;
import nandestech.dto.PeopleProtobufDeserializationSchema;
import nandestech.protos.EventStoreOuterClass;
import nandestech.protos.PeopleOuterClass;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventStoreProtobufApp {
    public static final String TOPIC = "event_store";
    public static final String BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create Kafka source
        KafkaSource<EventStoreOuterClass.EventStore> source =
                KafkaSource.<EventStoreOuterClass.EventStore>builder()
                        .setBootstrapServers(BROKERS)
                        .setProperty("partition.discovery.interval.ms", "10000")
                        .setTopics(TOPIC)
                        .setGroupId("groupId-protobuf-01")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new FlinkEventStoreProtobufDeserializationSchema())
                        .build();

        // Read data from Kafka
        DataStream<EventStoreOuterClass.EventStore> kafkaStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka");

        // Map Protobuf data to Tuple8
        DataStream<Tuple8<String, String, String, String, Integer, String, Integer, String>> processedStream =
                kafkaStream.map(event -> new Tuple8<>(
                        event.getEventStoreId(),
                        event.getEventName(),
                        event.getItemNumber(),
                        event.getItemType(),
                        event.getUserId(),
                        event.getUserLogin(),
                        event.getNodeId(),
                        event.getNodeCode()
                ));

        // Define JDBC sink
        processedStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO public.event_store (event_store_id, event_name, item_number, item_type, user_id, user_login, node_id, node_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (statement, tuple) -> {
                            statement.setString(1, tuple.f0);
                            statement.setString(2, tuple.f1);
                            statement.setString(3, tuple.f2);
                            statement.setString(4, tuple.f3);
                            statement.setInt(5, tuple.f4);
                            statement.setString(6, tuple.f5);
                            statement.setInt(7, tuple.f6);
                            statement.setString(8, tuple.f7);
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
                )
        );

        // Execute the Flink job
        env.execute("EventStore Protobuf to PostgreSQL");
    }
}
