package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;

public class KafkaSinkFunction {
    static final String SIEM_OUT_TOPIC =  "siem-data";

    static public KafkaSink<Tuple2<MetricsObject, Integer>> build(String kafkaBootstrapServers) {
        JsonSerializationSchema<Tuple2<MetricsObject, Integer>> jsonFormat = new JsonSerializationSchema<>();

        return KafkaSink.<Tuple2<MetricsObject, Integer>>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(SIEM_OUT_TOPIC)
                        .setValueSerializationSchema(jsonFormat)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .build();
    }
}
