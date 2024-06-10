package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class KafkaFlinkTopIP {

    public static void main(String[] args) throws Exception {
        new KafkaFlinkTopIP().process();
    }

    private void process() throws Exception {

        String kafkaBootstrapServers = System.getenv(
                "KAFKA_BOOTSTRAP_SERVERS");
        String kafkaGroupId = System.getenv("KAFKA_GROUP_ID");
        String kafkaTopics = System.getenv("KAFKA_TOPICS");
        String kafkaTopicPattern = System.getenv(
                "KAFKA_TOPIC_PATTERN");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceBuilder kafka =
                KafkaSource.<String>builder()
                        .setGroupId(kafkaGroupId)
                        .setProperty("partition.discovery.interval.ms", "30000")
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

        WatermarkStrategy<String> watermarkStrategy =
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5));

        final DataStream<String> inputStream =
                env.fromSource(kafkaSource,
                        watermarkStrategy, "Kafka Source");

        final OutputTag<KongLogTuple> out1
                = new OutputTag<KongLogTuple>("out-1") {
        };

        SingleOutputStreamOperator<Tuple2<KongLogRecord,
                Integer>> parsedStream = inputStream
                .process(new JsonParserProcessFunction()).name("Kafka Input Stream")
                .assignTimestampsAndWatermarks(new
                        MyAssignerWithPunctuatedWatermarks()).name("Watermarks")
                .process(new SplitProcessFunction(out1)).name("Split Output");

        GeoLocRichMapFunction geoLocation =
                new GeoLocRichMapFunction();

        buildSlidingAuthDataStream(kafkaBootstrapServers,
                parsedStream
                        .getSideOutput(out1), geoLocation);

        buildPrimaryStream(kafkaBootstrapServers,
                parsedStream, geoLocation);

        env.execute("Flink Kafka Top IPs");
    }

    static private void buildPrimaryStream(
            String kafkaBootstrapServers,
            SingleOutputStreamOperator<Tuple2<KongLogRecord, Integer>> parsedStream,
            GeoLocRichMapFunction geoLocation) {
        final OutputTag<Tuple2<KongLogRecord, Integer>> lateOutputTag =
                new OutputTag<Tuple2<KongLogRecord,
                        Integer>>("missed-window") {
                };

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(15));
        //TumblingProcessingTimeWindows processingTimeWindows = TumblingProcessingTimeWindows.of(Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String, Integer>>
                streamWindow = parsedStream
                .map(new InCounterMapFunction())
//                .assignTimestampsAndWatermarks(
//                    WatermarkStrategy.<Tuple2<KongLogRecord, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()))
                .keyBy(value -> WindowKey.getKey(value.f0))
                .window(tumblingEventTimeWindows)
                //.trigger(TimeAndWatermarkTrigger.of(5000))
                .sideOutputLateData(lateOutputTag)
                .aggregate(new CountAggregateFunction(),
                        new CountWindowFunction()).name
                        ("Tumbling Window Route Aggr");

        DataStream<Tuple2<MetricsObject, Integer>>
                resultStream
                = streamWindow
                .windowAll(tumblingEventTimeWindows)
                .process(new TopNProcessFunction(10))
                .name("Top N").setParallelism(1)
                .map(new
                        FlinkMetricsExposingMapFunction());

        resultStream.addSink(new Slf4jPrintSinkFunction
                ());
        resultStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-data")).name("Kafka Metrics Topic");

        DataStream<Tuple2<KongLogRecord, Integer>> lateStream =
                streamWindow
                        .getSideOutput(lateOutputTag);

        lateStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-late")).name("Kafka Late Topic");
    }

    private void buildSlidingAuthDataStream(
            String kafkaBootstrapServers,
            DataStream<KongLogTuple> inputStream,
            GeoLocRichMapFunction geoLocation) {

        final OutputTag<KongLogTuple> lateOutputTag =
                new OutputTag<KongLogTuple>("late-data") {
                };

        SlidingEventTimeWindows slidingEventTimeWindows =
                SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String,
                Integer>> streamWindow =
                inputStream
                        .filter(new
                                AuthSubRequestsOnlyFilterFunction())
                        .keyBy(value -> value.getKongLogRecord().getRequest().getHeaders().getAuthSub())
                        .window(slidingEventTimeWindows)
                        .reduce(new AuthIPReduceFunction())
                        .flatMap(new AuthFlattenMapFunction());

////                        .assignTimestampsAndWatermarks(
////                            WatermarkStrategy.<KongLogTuple>forBoundedOutOfOrderness(Duration.ofSeconds(0))
////                            .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()))
//                        .keyBy(value -> AuthSubWindowKey.getKey(value.getKongLogRecord()))
//                        .window(slidingEventTimeWindows)
//                        .sideOutputLateData(lateOutputTag)
//                        .aggregate(new CountLogTupleAggregateFunction(),
//                                new CountWindowFunction()).name("Sliding Window Auth Aggr");

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(15));

        DataStream<Tuple2<MetricsObject, Integer>> allWindow = streamWindow
                .windowAll(tumblingEventTimeWindows)
                .apply(new SlidingGroupByAllWindowFunction())
                .windowAll(tumblingEventTimeWindows)
                .process(new TopNAuthProcessFunction(10)).name("Top N").setParallelism(1)
                .map(geoLocation);

        allWindow.sinkTo(KafkaSinkFunction.build(kafkaBootstrapServers, "siem-auth"));

        DataStream<KongLogTuple> lateStream =
                streamWindow.getSideOutput(lateOutputTag);
        lateStream.addSink(new OverflowPrintSinkFunction());
    }
}
