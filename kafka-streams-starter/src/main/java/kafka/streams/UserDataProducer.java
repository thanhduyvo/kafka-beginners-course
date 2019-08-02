package kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // leverage idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // use Scanner to pace the example
        Scanner sc = new Scanner(System.in);

        // 1 - create a new user, then send some data to Kafka
        System.out.println("\nExample 1 - new user\n");
        producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get(); // get() to block the feature
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        // 2 - receive user purchase, but it doesn't exist in Kafka
        System.out.println("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob", "Kafka Starter (2)")).get();

        Thread.sleep(10000);

        // 3 - update user "john", and send a new transaction
        System.out.println("\nExample 3 - update to user\n");
        producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);

        // 4 - send a user purchase for duy, but it exists in Kafka later
        System.out.println("\nExample 4 - non existing user then user\n");
        producer.send(purchaseRecord("duy", "Computer (4)")).get();
        producer.send(userRecord("duy", "First=Duy,Last=Vo,GitHub=thanhduyvo")).get();

        Thread.sleep(10000);

        // 5 - create a user, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("alice", "First=Alice")).get();
        producer.send(userRecord("alice", null)).get(); // that's the delete record
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();

        Thread.sleep(10000);

        System.out.println("End of demo");

        // close producer
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }
}
