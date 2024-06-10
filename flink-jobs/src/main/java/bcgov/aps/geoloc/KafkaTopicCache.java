package bcgov.aps.geoloc;

import bcgov.aps.functions.JsonGeoParserFunction;
import bcgov.aps.functions.JsonParserProcessFunction;
import bcgov.aps.functions.KafkaSinkFunction;
import bcgov.aps.models.GeoLocInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.MetricsObject;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
public class KafkaTopicCache implements GeoLocService {
    static final String TOPIC = "siem-ip";

    private transient KafkaSink<GeoLocInfo> kafkaSink;
    private transient KafkaSource<String> kafkaSource;

    private transient Map<String, GeoLocInfo> ips;

    private transient GeoLocService geoLocService;

    private transient final StreamExecutionEnvironment env;

    public KafkaTopicCache(GeoLocService geoLocService) {
        this.ips = new HashMap<>();
        this.geoLocService = geoLocService;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrapServers = System.getenv(
                "KAFKA_BOOTSTRAP_SERVERS");
        kafkaSink = new KafkaSinkFunction<GeoLocInfo>().build(kafkaBootstrapServers, TOPIC);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSourceBuilder kafka =
                KafkaSource.<String>builder()
                        .setGroupId("siem")
                        .setProperty("partition.discovery.interval.ms", "30000")
                        .setBootstrapServers(kafkaBootstrapServers)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setTopics(TOPIC);
        kafkaSource = kafka.build();

        DataStream<String> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        SingleOutputStreamOperator<GeoLocInfo> out
                = stream.process(new JsonGeoParserFunction()).name("Kafka Input Stream");

        out.addSink(new SinkFunction <GeoLocInfo>() {
            @Override
            public void invoke(GeoLocInfo value, Context context) {
                log.info("New IP Caching {}", value.getIp());
                ips.put(value.getIp(), value);
            }
        });
    }

    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException, ExecutionException {
        GeoLocInfo geo = ips.get(ip);
        if (geo == null) {
            log.info("[KafkaTopicCache] MISS {}", ip);
            geo = geoLocService.fetchGeoLocationInformation(ip);
            if (geo != null) {
                DataStream<GeoLocInfo> stream = env.fromElements(geo);
                stream.sinkTo(kafkaSink);
            }
        } else {
            log.info("[KafkaTopicCache] HIT  {}", ip);
        }
        return geo;
    }

}
