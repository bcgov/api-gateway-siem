package bcgov.aps.streams;

import bcgov.aps.functions.*;
import bcgov.aps.models.AuthSubWindowKey;
import bcgov.aps.models.KongLogTuple;
import bcgov.aps.models.MetricsObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SlidingAuthStream {
    public void build(
            String kafkaBootstrapServers,
            DataStream<KongLogTuple> inputStream) {

        final OutputTag<KongLogTuple> lateOutputTag =
                new OutputTag<KongLogTuple>("late-data") {
                };

        GeoLocRichMapFunction geoLocation =
                new GeoLocRichMapFunction();

        SlidingEventTimeWindows slidingEventTimeWindows =
                SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String,
                Integer>> streamWindow =
                inputStream
                        .filter(new
                                AuthSubRequestsOnlyFilterFunction())
                        .keyBy(value -> AuthSubWindowKey.getAuthSub(value.getKongLogRecord()).f0)
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
                .process(new TopNAuthProcessFunction(50)).name("Top N").setParallelism(1)
                .map(geoLocation);

        allWindow.sinkTo(KafkaSinkFunction.build(kafkaBootstrapServers, "siem-auth"));

        DataStream<KongLogTuple> lateStream =
                streamWindow.getSideOutput(lateOutputTag);
        lateStream.addSink(new OverflowPrintSinkFunction());
    }
}
