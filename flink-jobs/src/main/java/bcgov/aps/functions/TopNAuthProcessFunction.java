package bcgov.aps.functions;

import bcgov.aps.KafkaFlinkTopIP;
import bcgov.aps.models.AuthWindowKey;
import bcgov.aps.models.GeoLocInfo;
import bcgov.aps.models.MetricsObject;
import bcgov.aps.models.WindowKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.PriorityQueue;

@Slf4j
public class TopNAuthProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<MetricsObject, Integer>, TimeWindow> {
    private final int topSize;

    public TopNAuthProcessFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void process(Context context,
                        Iterable<Tuple2<String
                                , Integer>> elements,
                        Collector<Tuple2<MetricsObject,
                                Integer>> out) {
        PriorityQueue<Tuple2<String, Integer>> topN =
                new PriorityQueue<>(Comparator.comparingInt(o -> o.f1));

        int other = 0;
        int ipCount = 0;
        int requestCount = 0;
        for (Tuple2<String, Integer> element :
                elements) {
            ipCount++;
            log.info("Element {}", element);
            if (element.f1 == 0) {
                log.warn("Element has zero so skip it. {}", element.f0);
                continue;
            }
            topN.add(element);
            if (topN.size() > topSize) {
                Tuple2<String, Integer> kickedOff
                        = topN.poll();
                if (kickedOff != null) {
                    other += kickedOff.f1;
                }
            }
            requestCount += element.f1;
        }
        log.info("TopNAuth {} {} | {} -> {} : {}", ipCount, topN.size(), context.window().getStart(), context.window().getEnd(), context.window().maxTimestamp());

        for (Tuple2<String, Integer> entry :
                topN) {
            MetricsObject met = AuthWindowKey.parseKey(entry.f0);
            met.setWindowTime(context.window().getEnd());
            out.collect(new Tuple2<>(met, entry.f1));
        }
    }
}
