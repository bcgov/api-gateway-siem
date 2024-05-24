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
        matches.put("other", 0);
        matches.put("0.1.2.3", 1);
        matches.put("0.2.2.1", 1);
        matches.put("0.2.2.2", 1);
        matches.put("0.2.2.3", 6);
        matches.put("0.2.2.4", 1);
    }

    @Override
    public void invoke(Tuple2<MetricsObject, Integer> value, Context context) {
        String ip = value.f0.getClientIp();
        assertion (matches.containsKey(ip), String.format("Missing %s", ip));
        assertion (matches.get(ip) == value.f1, String.format("Mismatch %s %d", ip, value.f1));
    }

    private void assertion (boolean v, String message) {
        if (!v) {
            throw new RuntimeException("Assertion failed - " + message);
        }
    }

}
