package bcgov.aps.streams;

import bcgov.aps.functions.*;
import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.WindowKey;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class ProcessTopNIPStream {
    public void build(
            String kafkaBootstrapServers,
            SingleOutputStreamOperator<Tuple2<KongLogRecord, Integer>> parsedStream) {

        final OutputTag<Tuple2<KongLogRecord, Integer>> lateOutputTag =
                new OutputTag<Tuple2<KongLogRecord,
                        Integer>>("missed-process-window") {
                };

        GeoLocRichMapFunction geoLocation =
                new GeoLocRichMapFunction();

        TumblingProcessingTimeWindows timeWindow = TumblingProcessingTimeWindows.of(Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String, Integer>>
                streamWindow = parsedStream
                .map(new InCounterMapFunction())
                .keyBy(value -> WindowKey.getKey(value.f0))
                .window(timeWindow)
                .sideOutputLateData(lateOutputTag)
                .aggregate(new CountAggregateFunction(),
                        new CountWindowFunction()).name
                        ("Tumbling Window Route Aggr");

        DataStream<Tuple2<MetricsObject, Integer>>
                resultStream
                = streamWindow
                .windowAll(timeWindow)
                .process(new TopNProcessFunction(10))
                .name("Top N").setParallelism(1)
                .map(geoLocation);

        resultStream.addSink(new Slf4jPrintSinkFunction
                ());
        resultStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-process-data")).name("Kafka Metrics Topic");

        DataStream<Tuple2<KongLogRecord, Integer>> lateStream =
                streamWindow
                        .getSideOutput(lateOutputTag);

        lateStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-late-process")).name("Kafka Late Topic");
    }
}
