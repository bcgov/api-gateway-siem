package bcgov.aps;

import bcgov.aps.functions.*;
import bcgov.aps.models.MetricsObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;

public class TestFileTopIP {
    private static final Logger LOG =
            LoggerFactory.getLogger(TestFileTopIP.class);

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

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(15));

        inputStream
                .process(new JsonParserProcessFunction())
                .assignTimestampsAndWatermarks(new
                 MyAssignerWithPunctuatedWatermarks())
                .keyBy(value -> value.f0.getClientIp())
                .window(tumblingEventTimeWindows);
//                .aggregate(new CountAggregateFunction(),
//                        new CountWindowFunction());
//
//        DataStream<Tuple2<MetricsObject, Integer>> resultStream
//                = parsedStream
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(30)))
//                .process(new TopNProcessFunction(10))
//                .map(new FlinkMetricsExposingMapFunction());
//
//        resultStream.addSink(new Slf4jPrintSinkFunction());
//
//        resultStream.addSink(new AssertSinkFunction<>());

        env.execute("Flink Kafka Top IPs");
    }
}
