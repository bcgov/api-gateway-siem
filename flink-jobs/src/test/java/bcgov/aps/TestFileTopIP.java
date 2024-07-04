package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.net.URI;
import java.time.Duration;

@Slf4j
public class TestFileTopIP {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setMaxParallelism(1);
//        Properties props = new Properties();
//        props.setProperty("metrics.reporter.prom
//        .factory.class", "org.apache.flink.metrics
//        .prometheus.PrometheusReporterFactory");
//        props.setProperty("metrics.reporter.prom.port",
//        "9250-9260");
//
//        env.getConfig().toConfiguration()
//        .addAllToProperties(props);

        //        env.setRestartStrategy
        //        (RestartStrategies.fixedDelayRestart
        //        (1000, 1000));
        //        env.setParallelism(2);
        //        env.getConfig()
        //        .setAutoWatermarkInterval(1000);
        //        env.setStreamTimeCharacteristic
        //        (TimeCharacteristic.EventTime);

        final DataStream<String> inputStream;

        URI filePath = TestFileTopIP.class.getResource(
                "/test.log").toURI();

        final FileSource<String> fileSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath))
                        .build();
        inputStream =
                env.fromSource(fileSource,
                        WatermarkStrategy.forMonotonousTimestamps(), "whatever");

        SlidingEventTimeWindows slidingEventTimeWindows =
                SlidingEventTimeWindows.of(Duration.ofSeconds(30), Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamWindow = inputStream
                .process(new JsonParserProcessFunction())
                .assignTimestampsAndWatermarks(new
                 MyAssignerWithPunctuatedWatermarks())
                .map(new KongLogMapFunction())
                .filter(new
                        AuthSubRequestsOnlyFilterFunction())
                .keyBy(value -> value.getKongLogRecord().getRequest().getHeaders().getAuthSub())
                .window(slidingEventTimeWindows)
                .reduce(new AuthIPReduceFunction())
                .flatMap(new AuthFlattenMapFunction());

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(15));

        DataStream<Tuple2<MetricsObject, Integer>> allWindow = streamWindow
                .windowAll(tumblingEventTimeWindows)
                .apply(new SlidingGroupByAllWindowFunction())
                .windowAll(tumblingEventTimeWindows)
                .process(new TopNProcessFunction(10)).name("Top N").setParallelism(1);

        allWindow.addSink(new Slf4jPrintSinkFunction());

        allWindow.addSink(new AssertSinkFunction<>());

        env.execute("Flink Kafka Top IPs");
    }
}
