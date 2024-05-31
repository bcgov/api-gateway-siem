package bcgov.aps.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SlidingGroupByAllWindowFunction implements AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
        Map<String, Integer> resultMap =
                new HashMap<>();
        for (Tuple2<String, Integer> value : values) {
            resultMap.merge(value.f0,
                    value.f1, Integer::sum);
        }
        resultMap.forEach((k, v) -> {
            out.collect(new Tuple2<>(k, v));
        });

    }
}
