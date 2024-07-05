package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.*;
import bcgov.aps.streams.ProcessTopNIPStream;
import bcgov.aps.streams.SlidingAuthStream;
import bcgov.aps.streams.TopNErrorsIPStream;
import bcgov.aps.streams.TopNIPStream;
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
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OutputTag;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.internal.io.ResourceFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
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
        Config config = new Config();

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceBuilder<String> kafka =
                KafkaSource.<String>builder()
                        .setGroupId(config.getKafkaGroupId())
                        .setProperty("partition.discovery.interval.ms", "30000")
                        .setBootstrapServers(config.getKafkaBootstrapServers())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema());

        if (StringUtils.isNotBlank(config.getKafkaTopics())) {
            log.info("Topics {}", StringUtils.join(
                    config.getKafkaTopics(), '|'));
            List<String> _kafkaTopics =
                    Arrays.asList(config.getKafkaTopics().split(","));
            kafka.setTopics(_kafkaTopics);
        } else {
            log.info("Topic Pattern {}", config.getKafkaTopicPattern());
            kafka.setTopicPattern(Pattern.compile(config.getKafkaTopicPattern()));
        }

        KafkaSource<String> kafkaSource = kafka.build();

        WatermarkStrategy<String> watermarkStrategy =
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(config.getMaxOutOfOrderness()));

        final DataStream<String> inputStream =
                env.fromSource(kafkaSource,
                        watermarkStrategy, "Kafka Source");

        final OutputTag<KongLogTuple> out1
                = new OutputTag<KongLogTuple>("out-1") {};
        final OutputTag<KongLogTuple> out2
                = new OutputTag<KongLogTuple>("out-2") {};
        final OutputTag<KongLogTuple> out3
                = new OutputTag<KongLogTuple>("out-3") {};

        SingleOutputStreamOperator<Tuple2<KongLogRecord,
                Integer>> parsedStream = inputStream
                .process(new JsonParserProcessFunction()).name("Kafka Input Stream")
                .assignTimestampsAndWatermarks(new
                        MyAssignerWithPunctuatedWatermarks()).name("Watermarks")
                .process(new SplitProcessFunction(out1, out2, out3)).name("Split Output");

        new SlidingAuthStream().build(config.getKafkaBootstrapServers(),
                parsedStream
                        .getSideOutput(out1));

        new TopNIPStream().build(config.getKafkaBootstrapServers(),
                parsedStream
                        .getSideOutput(out2));

        new TopNErrorsIPStream().build(config.getKafkaBootstrapServers(),
                parsedStream
                        .getSideOutput(out3));

        String rules = FileUtils.readFileUtf8(new File("./rules.drl"));
        new ProcessTopNIPStream().build(rules, config.getKafkaBootstrapServers(),
                parsedStream);

        env.execute("Flink Kafka Top IPs");
    }




}
