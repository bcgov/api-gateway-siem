package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class KafkaFlinkTopIP {
    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaFlinkTopIP.class);

    public static void main(String[] args) throws Exception {

        String kafkaBootstrapServers = System.getenv(
                "KAFKA_BOOTSTRAP_SERVERS");
        List<String> kafkaTopics =
                Arrays.asList(System.getenv("KAFKA_TOPICS"
                ).split(","));

        LOG.warn("Topics {}", StringUtils.joinWith("|",
                kafkaTopics));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setMaxParallelism(1);

        final DataStream<String> inputStream;

        SlidingEventTimeWindows slidingEventTimeWindows =
                SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(30));

        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrapServers)
                        .setTopics(kafkaTopics)
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        inputStream = env.fromSource(kafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        DataStream<Tuple2<MetricsObject, Integer>> parsedStream = inputStream
                .process(new JsonParserProcessFunction())
                .keyBy(value -> value.f0.getClientIp())
                .window(slidingEventTimeWindows)
                .aggregate(new CountAggregateFunction(),
                        new CountWindowFunction());

        DataStream<Tuple2<MetricsObject, Integer>> resultStream
                = parsedStream
                .windowAll(slidingEventTimeWindows)
                .process(new TopNProcessFunction(10))
                .map(new FlinkMetricsExposingMapFunction());

        resultStream.addSink(new Slf4jPrintSinkFunction<>());

        env.execute("Flink Kafka Top IPs");
    }
}
