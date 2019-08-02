package kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class UserEventEnricherApp {

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // get a global table out of Kafka
        GlobalKTable<String, String> usersGlobalTable = streamsBuilder.globalTable("user-table");

        // get a stream of user purchases
        KStream<String, String> userPurchases = streamsBuilder.stream("user-purchases");

        // enrich that stream using inner join
        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(usersGlobalTable, (key, value) -> key,
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]");

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // enrich that stream using left join
        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(usersGlobalTable, (key, value) -> key,
                (userPurchase, userInfo) -> {
                    // userInfo can be null
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                    } else {
                        return "Purchase=" + userPurchase + ",UserInfo=null";
                    }
                });

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

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
