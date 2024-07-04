package bcgov.aps.streams;

import bcgov.aps.functions.*;
import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.WindowKey;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TopNIPStream {
    public void build(
            String kafkaBootstrapServers,
            DataStream<KongLogTuple> parsedStream) {

        final OutputTag<KongLogTuple> lateOutputTag =
                new OutputTag<KongLogTuple>("missed-window") {
                };

        GeoLocRichMapFunction geoLocation =
                new GeoLocRichMapFunction();

        TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(15));
        //TumblingProcessingTimeWindows processingTimeWindows = TumblingProcessingTimeWindows.of(Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String, Integer>>
                streamWindow = parsedStream
//                .assignTimestampsAndWatermarks(
//                    WatermarkStrategy.<Tuple2<KongLogRecord, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()))
                .keyBy(value -> WindowKey.getKey(value.getKongLogRecord()))
                .window(tumblingEventTimeWindows)
                //.trigger(TimeAndWatermarkTrigger.of(5000))
                .sideOutputLateData(lateOutputTag)
                .aggregate(new CountLogTupleAggregateFunction(),
                        new CountWindowFunction()).name
                        ("Tumbling Window Route Aggr");

        DataStream<Tuple2<MetricsObject, Integer>>
                resultStream
                = streamWindow
                .windowAll(tumblingEventTimeWindows)
                .process(new TopNProcessWithMetricsFunction(10))
                .name("Top N").setParallelism(1)
                .map(geoLocation)
                .map(new
                        FlinkMetricsExposingMapFunction());

        resultStream.addSink(new Slf4jPrintSinkFunction
                ());
        resultStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-data")).name("Kafka Metrics Topic");

        DataStream<KongLogTuple> lateStream =
                streamWindow
                        .getSideOutput(lateOutputTag);

        lateStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-late")).name("Kafka Late Topic");
    }
}
