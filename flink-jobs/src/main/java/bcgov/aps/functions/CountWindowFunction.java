package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@Slf4j
public class CountWindowFunction implements WindowFunction<Integer, Tuple2<MetricsObject, Integer>, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window,
                      Iterable<Integer> input,
                      Collector<Tuple2<MetricsObject,
                              Integer>> out) throws Exception {
        int count = input.iterator().next();
        log.debug("CountWindow {} {} {}",
                window.maxTimestamp(), key, count);
        MetricsObject o = new MetricsObject();
        o.setClientIp(key);
        out.collect(new Tuple2<>(o, count));
    }
}
