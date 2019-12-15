import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> stream = builder.stream("twitter_tweets");
        KStream<String,String> filterStream = stream.filter(
                (k,v) -> filterByFollowers(v) > 10000
        );
        filterStream.to("popular_tweets");

        KafkaStreams kafkaStream = new KafkaStreams(builder.build(),prop);

        kafkaStream.start();
    }

    private static int filterByFollowers(String record) {
        JsonParser parse = new JsonParser();
        return parse.parse(record).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
    }
}
