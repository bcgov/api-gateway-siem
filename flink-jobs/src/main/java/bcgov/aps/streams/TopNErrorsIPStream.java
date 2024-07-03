package bcgov.aps.streams;

import bcgov.aps.functions.*;
import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.WindowKey;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TopNErrorsIPStream {
    public void build(
            String kafkaBootstrapServers,
            DataStream<KongLogTuple> parsedStream) {

        GeoLocRichMapFunction geoLocation =
                new GeoLocRichMapFunction();

        TumblingProcessingTimeWindows timeWindow = TumblingProcessingTimeWindows.of(Duration.ofSeconds(15));

        SingleOutputStreamOperator<Tuple2<String, Integer>>
                streamWindow = parsedStream
                .filter(new
                        OnlyErrorsFilterFunction())
                .keyBy(value -> WindowKey.getKey(value.getKongLogRecord()))
                .window(timeWindow)
                .aggregate(new CountLogTupleAggregateFunction(),
                        new CountWindowFunction()).name
                        ("Tumbling Window Route Aggr");

        DataStream<Tuple2<MetricsObject, Integer>>
                resultStream
                = streamWindow
                .windowAll(timeWindow)
                .process(new TopNProcessFunction(15))
                .name("Top N").setParallelism(1)
                .map(geoLocation);

        resultStream.addSink(new Slf4jPrintSinkFunction
                ());
        resultStream.sinkTo(KafkaSinkFunction.build
                (kafkaBootstrapServers, "siem-error-data")).name("Kafka Metrics Topic");


    }
}
