package consumer;

import nandestech.protos.EventStoreOuterClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaEventStoreProtobufConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "protobufEventStore");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("event_store"));

        System.out.println("Listening for messages...");
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            try {
                for (ConsumerRecord<String, byte[]> record : records) {
                    EventStoreOuterClass.EventStore eventStore = EventStoreOuterClass.EventStore.parseFrom(record.value());
                    System.out.printf("Received: Event Store ID=%s, Event Name=%s, Item Number=%s, Item Type=%s, User ID=%s, User Login=%s, Node Code=%s, Created At=%s%n",
                            eventStore.getEventStoreId(), eventStore.getEventName(), eventStore.getItemNumber(), eventStore.getItemType(), eventStore.getUserId(), eventStore.getUserLogin(), eventStore.getNodeCode(), eventStore.getCreatedAt());
                }
            } catch (Exception e) {
                System.out.println("Failed to deserialize Protobuf message: " + e.getMessage());
            }
        }
    }
}
