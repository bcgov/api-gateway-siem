package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class Slf4jPrintSinkFunction<T> implements SinkFunction<Tuple2<MetricsObject, Integer>> {

    @Override
    public void invoke(Tuple2<MetricsObject, Integer> value, Context context) {
        log.error("Sinked: [{}] {} : {}",
                context.timestamp(), value.f0.getClientIp(), value.f1);
    }
}
