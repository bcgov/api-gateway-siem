package bcgov.aps.functions;

import bcgov.aps.models.AuthSubWindowKey;
import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

@Slf4j
public class TopNAuthProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<MetricsObject, Integer>, TimeWindow> {
    private final int topSize;

    private Long lastMetricTs;

    private List<Tuple2<String, Integer>> lastWindow;

    public TopNAuthProcessFunction(int topSize) {
        this.topSize = topSize;
        this.lastWindow = new ArrayList<>();
        this.lastMetricTs = Long.valueOf(0L);
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
            log.debug("Element {}", element);
            if (element.f1 == 0) {
                log.warn("Element has zero so skip it. " +
                        "{}", element.f0);
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
        log.debug("TopNAuth {} {} | {} -> {} : {}",
                ipCount, topN.size(),
                context.window().getStart(),
                context.window().getEnd(),
                context.window().maxTimestamp());

        processLastWindow(context.window(), topN, out);

        for (Tuple2<String, Integer> entry :
                topN) {
            MetricsObject met =
                    AuthSubWindowKey.parseKey(entry.f0);
            met.setWindowTime(context.window().getEnd());
            out.collect(new Tuple2<>(met, entry.f1));
        }
    }

    /*
        Keep track of the last window, and send out zero
        values for the
        metrics data for that so that the "gauge" is
        reflected properly
     */
    private void processLastWindow(TimeWindow window,
                                   PriorityQueue<Tuple2<String, Integer>> topN, Collector<Tuple2<MetricsObject,
            Integer>> out) {
        if (lastMetricTs == window.getEnd()) {
            log.warn("[processLastWindow] not expecting " +
                    "end window to appear twice!");
            return;
        }
        if (lastMetricTs != 0 && lastMetricTs != window.getEnd()) {
            for (Tuple2<String, Integer> entry :
                    topN) {
                MetricsObject met =
                        AuthSubWindowKey.parseKey(entry.f0);
                met.setWindowTime(window.getEnd());
                out.collect(new Tuple2<>(met, 0));
            }
        }
        lastWindow.clear();
        lastWindow.addAll(topN);
        lastMetricTs = window.getEnd();
    }
}
