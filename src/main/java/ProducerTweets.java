import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class ProducerTweets {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTweets.class);
    private static final String TOPIC_NAME = "raw-tweets";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        String apiKey = args[0];
        String apiSecret = args[1];
        String tokenValue = args[2];
        String tokenSecret = args[3];

        final KafkaProducer<String, String> prod = configureKafkaProducer();

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthAccessToken(tokenValue);
        configurationBuilder.setOAuthAccessTokenSecret(tokenSecret);
        configurationBuilder.setOAuthConsumerKey(apiKey);
        configurationBuilder.setOAuthConsumerSecret(apiSecret);
        configurationBuilder.setJSONStoreEnabled(true);
        configurationBuilder.setIncludeEntitiesEnabled(true);

        final TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        try {
            StatusListener statusListener = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    HashtagEntity[] hashtagEntities = status.getHashtagEntities();
                    if (hashtagEntities.length > 0) {
                        String value = TwitterObjectFactory.getRawJSON(status);
                        String language = status.getLang();
                        prod.send(new ProducerRecord<>(ProducerTweets.TOPIC_NAME, language, value));
                    }
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    // default implementation ignored
                }

                @Override
                public void onTrackLimitationNotice(int i) {
                    // default implementation ignored
                }

                @Override
                public void onScrubGeo(long l, long l1) {
                    // default implementation ignored
                }

                @Override
                public void onStallWarning(StallWarning stallWarning) {
                    // default implementation ignored
                }

                @Override
                public void onException(Exception e) {
                    // default implementation ignored
                }
            };

            twitterStream.addListener(statusListener);
            twitterStream.sample();
        } catch (Exception e) {
            LOGGER.error("Error when consuming streams from the Twitter API. Error details: {}", e.getMessage());
        }

    }

    private static KafkaProducer<String, String> configureKafkaProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        return new KafkaProducer<>(props);
    }

}
