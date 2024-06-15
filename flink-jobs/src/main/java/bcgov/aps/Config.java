package bcgov.aps;

import lombok.Getter;

@Getter
public class Config {
    public Config() {
        kafkaBootstrapServers = System.getenv(
                "KAFKA_BOOTSTRAP_SERVERS");
        kafkaGroupId = System.getenv("KAFKA_GROUP_ID");

        if (kafkaGroupId == null) {
            kafkaGroupId = "siem";
        }
        kafkaTopics = System.getenv("KAFKA_TOPICS");
        kafkaTopicPattern = System.getenv(
                "KAFKA_TOPIC_PATTERN");
        String maxOutOfOrdernessString = System.getenv("FLINK_MAX_OUT_OF_ORDERNESS");
        if (maxOutOfOrdernessString == null) {
            maxOutOfOrdernessString = "5";
        }
        maxOutOfOrderness = Integer.valueOf(maxOutOfOrdernessString);
    }

    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private String kafkaTopics;
    private String kafkaTopicPattern;

    private int maxOutOfOrderness;

}
