package producer;

import nandestech.model.People;
import nandestech.protos.PeopleOuterClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProtobufProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        PeopleOuterClass.People people = PeopleOuterClass.People.newBuilder()
                .setName("John Doe")
                .setTimestamp("2022-11-07 20:58:57.610088")
                .setCountry("USA")
                .setJob("Software Engineer")
                .setImage("https://picsum.photos/954/374")
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("people_protobuf", people.toByteArray());
//        producer.send(record);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error while producing: " + exception.getMessage());
            } else {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }).get();
        producer.close();
    }
}
