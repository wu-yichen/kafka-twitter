import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class twitterProducer {

    public static void main(String[] args) throws IOException {
        new twitterProducer().run();
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("lolita");
        hosebirdEndpoint.trackTerms(terms);
        Properties prop = Helper.getPropValues();
        String consumerKey = prop.getProperty("consumerKey");
        String consumerSecret = prop.getProperty("consumerSecret");
        String token = prop.getProperty("token");
        String tokenSecret = prop.getProperty("tokenSecret");
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
        ClientBuilder builder = new ClientBuilder();
        builder.hosts(hosebirdHosts);
        builder.authentication(hosebirdAuth);
        builder.endpoint(hosebirdEndpoint);
        builder.processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    private void run() throws IOException {

        Logger logger = LoggerFactory.getLogger(twitterProducer.class.getName());
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        // create twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();
        // create kafka producer
        KafkaProducer<String, String> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.stop();
            producer.close();
            logger.info("application exited");
        }));

        while (!twitterClient.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info(msg + "\n");
                    producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                        if (e != null) {
                            logger.error("something is wrong", e);
                        }
                    });
                }
            } catch (InterruptedException e) {
                twitterClient.stop();
                producer.close();
            }
        }
    }

    private KafkaProducer<String, String> createProducer() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // safer producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        return new KafkaProducer<>(properties);
    }
}
