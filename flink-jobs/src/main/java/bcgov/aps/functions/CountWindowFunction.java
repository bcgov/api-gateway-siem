package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@Slf4j
public class CountWindowFunction implements WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window,
                      Iterable<Integer> input,
                      Collector<Tuple2<String,
                              Integer>> out) {
        int count = input.iterator().next();

        log.debug("CountWindow {} {} {} {}",
                input.iterator().hasNext(), window.maxTimestamp(), key, count);
        out.collect(new Tuple2<>(key, count));
    }
}
