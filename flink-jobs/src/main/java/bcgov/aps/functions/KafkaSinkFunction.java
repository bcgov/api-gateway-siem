package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

public class KafkaSinkFunction<T> {

    static public KafkaSink build(String kafkaBootstrapServers, String topic) {
        JsonSerializationSchema jsonFormat = new JsonSerializationSchema<>((
                () -> new ObjectMapper()
                        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)));

        return KafkaSink.builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(jsonFormat)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .build();
    }
}
