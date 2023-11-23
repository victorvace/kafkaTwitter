import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class TweetsHashtagsCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetsHashtagsCounter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TOPIC_NAME = "raw-tweets";
    private static final String APPLICATION_ID = "tweethashtagscounterappkk";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> tweets = streamsBuilder.stream(TOPIC_NAME);

        tweets.flatMapValues(value -> Collections.singletonList(extractHashtags(value)))
                .groupBy((key, value) -> value).count().toStream().print(Printed.toSysOut());

        KafkaStreams kafkaStreams = configureKafkaStreams(streamsBuilder);
        kafkaStreams.start();

    }

    private static KafkaStreams configureKafkaStreams(StreamsBuilder streamsBuilder) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreams(streamsBuilder.build(), props);
    }

    private static String extractHashtags(String input) {
        try {
            JsonNode jsonNodeRoot = objectMapper.readTree(input);
            JsonNode hashtagsJsonNode = jsonNodeRoot.path("entities").path("hashtags");
            if (!hashtagsJsonNode.isEmpty()) {
                return hashtagsJsonNode.get(0).path("text").asText();
            }
        } catch (Exception e) {
            LOGGER.error("Error when consuming streams from the Twitter API. Error details: {}", e.getMessage());
        }

        return "";
    }

}
