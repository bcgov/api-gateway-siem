package bcgov.aps;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AssertSinkFunction<T> implements SinkFunction<Tuple2<MetricsObject, Integer>> {

    static final Map<String, Integer> matches = new HashMap<>();
    static {
        matches.put("9.9.2.3#10.9.2.3 sub-5678", 2);
        matches.put("9.9.2.3#0.1.2.3 sub-1234", 3);
    }

    @Override
    public void invoke(Tuple2<MetricsObject, Integer> value, Context context) {
        String key = value.f0.getClientIp() + " " + value.f0.getAuthSub();
        assertion (matches.containsKey(key), String.format("Missing %s", key));
        assertion (matches.get(key) == value.f1, String.format("Mismatch %s %d", key, value.f1));
    }

    private void assertion (boolean v, String message) {
        if (!v) {
            throw new RuntimeException("Assertion failed - " + message);
        }
    }

}
