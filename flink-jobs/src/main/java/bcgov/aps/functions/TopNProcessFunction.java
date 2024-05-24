package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
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
public class TopNProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<MetricsObject, Integer>, TimeWindow> {
    private final int topSize;

    private int totalIps;

    private int totalRequests;

    @Override
    public void open(Configuration parameters) throws Exception {
        OperatorMetricGroup metricGroup =
                getRuntimeContext().getMetricGroup();

        metricGroup
                .gauge("aps_siem_ip",
                        new Gauge<Integer>() {
                            @Override
                            public Integer getValue() {
                                return totalIps;
                            }
                        });
        metricGroup
                .gauge("aps_siem_requests",
                        new Gauge<Integer>() {
                            @Override
                            public Integer getValue() {
                                return totalRequests;
                            }
                        });
    }

    public TopNProcessFunction(int topSize) {
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
        totalIps = ipCount;
        totalRequests = requestCount;

        log.info("TopNSize {}", topN.size());

        for (Tuple2<String, Integer> entry :
                topN) {
            MetricsObject met = WindowKey.parseKey(entry.f0);
            met.setWindowTime(context.window().maxTimestamp());
            out.collect(new Tuple2<>(met, entry.f1));
        }

        if (other != 0) {
            MetricsObject met = new MetricsObject();
            met.setClientIp("other");
            met.setWindowTime(context.window().maxTimestamp());
            met.setStatus(MetricsObject.HTTP_STATUS.NA); // not totally accurate - count includes all requests :(
            out.collect(new Tuple2<>(met, other));
        }
    }
}
