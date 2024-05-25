package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.WindowKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class KafkaFlinkTopIP {
    public static void main(String[] args) throws Exception {

        String kafkaBootstrapServers = System.getenv(
                "KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopics = System.getenv("KAFKA_TOPICS");
        String kafkaTopicPattern = System.getenv("KAFKA_TOPIC_PATTERN");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setMaxParallelism(1);
        //env.getConfig().setAutoWatermarkInterval(1000);

        final DataStream<String> inputStream;

        KafkaSourceBuilder kafka =
                KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaBootstrapServers)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema());

        if (StringUtils.isNotBlank(kafkaTopics)) {
            log.info("Topics {}", StringUtils.join(
                    kafkaTopics, '|'));
            List<String> _kafkaTopics =
                    Arrays.asList(kafkaTopics.split(","));
            kafka.setTopics(_kafkaTopics);
        } else {
            log.info("Topic Pattern {}", kafkaTopicPattern);
            kafka.setTopicPattern(Pattern.compile(kafkaTopicPattern));
        }

        KafkaSource<String> kafkaSource = kafka.build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();//.noWatermarks();//.forBoundedOutOfOrderness(Duration.ofSeconds(10));

//        SlidingEventTimeWindows slidingEventTimeWindows =
//                SlidingEventTimeWindows.of(Duration.ofSeconds(30), Duration.ofSeconds(30));

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(5));

        inputStream = env.fromSource(kafkaSource,
                watermarkStrategy, "Kafka Source");

        DataStream<Tuple2<String, Integer>> parsedStream = inputStream
                .process(new JsonParserProcessFunction())
                .map(new InCounterMapFunction())
//                .assignTimestampsAndWatermarks(new
//                        MyAssignerWithPunctuatedWatermarks())
                .keyBy(value -> WindowKey.getKey(value.f0))
                .window(tumblingEventTimeWindows)
                .aggregate(new CountAggregateFunction(),
                        new CountWindowFunction());

        DataStream<Tuple2<MetricsObject, Integer>> resultStream
                = parsedStream
                .windowAll(tumblingEventTimeWindows)
                .process(new TopNProcessFunction(10)).setParallelism(1)
                .map(new FlinkMetricsExposingMapFunction())
                .map(new GeoLocRichMapFunction());

        resultStream.addSink(new Slf4jPrintSinkFunction());

        resultStream.sinkTo(KafkaSinkFunction.build(kafkaBootstrapServers));

        env.execute("Flink Kafka Top IPs");
    }
}
