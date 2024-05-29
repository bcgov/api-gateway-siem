package bcgov.aps.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

@Slf4j
public class CountAllWindowFunction implements AllWindowFunction<Integer, Tuple2<String, Integer>, Window> {

//    @Override
//    public void apply(String key,
//                      Iterable<Integer> input,
//                      Collector<Tuple2<String, Integer>> out) throws Exception {
//        int count = input.iterator().next();
//        out.collect(new Tuple2<>(key, count));
//    }

    @Override
    public void apply(Window window,
                      Iterable<Integer> input,
                      Collector<Tuple2<String, Integer>> collector) throws Exception {
    }
}
