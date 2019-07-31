package kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourApp {

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // disable the cache to demonstrate all the "steps" involved in the transformation
        properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // step 1: create the topic of users keys to colours
        KStream<String, String> textLines = streamsBuilder.stream("favourite-colour-input");

        KStream<String, String> usersAndColours = textLines
                // 1 - ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - filter undesired colours
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        // 5 - write the results back to kafka
        usersAndColours.to("user-keys-and-colours");

        // step 2: read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = streamsBuilder.table("user-keys-and-colours");

        // step 3: count the occurrences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 6 - group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.as("CountsByColours"));

        // 7 - output the results to a Kafka topic with serializers
        favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams application
        kafkaStreams.cleanUp(); // only do this in Dev, not in Prod
        kafkaStreams.start();

        // print the topology
        System.out.println(streamsBuilder.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
