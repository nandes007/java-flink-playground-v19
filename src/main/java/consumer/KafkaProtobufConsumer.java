package consumer;

import nandestech.protos.PeopleOuterClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaProtobufConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "protobufConsumerGroup01");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("people_protobuf"));

        System.out.println("Listening for messages...");
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            try {
                for (ConsumerRecord<String, byte[]> record : records) {
                    PeopleOuterClass.People people = PeopleOuterClass.People.parseFrom(record.value());
                    System.out.printf("Received: Name=%s, Timestamp=%s, Country=%s, Job=%s, Image=%s%n",
                            people.getName(), people.getTimestamp(), people.getCountry(), people.getJob(), people.getImage());
                }
            } catch (Exception e) {
                System.err.println("Failed to deserialize Protobuf message: " + e.getMessage());
            }
        }
    }
}
