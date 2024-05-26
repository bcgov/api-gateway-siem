package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Date;

@Slf4j
public class Slf4jPrintSinkFunction implements SinkFunction<Tuple2<MetricsObject, Integer>> {

    private long lastTs;


    public Slf4jPrintSinkFunction() {
        super();
        lastTs = new Date().getTime();
    }

    @Override
    public void invoke(Tuple2<MetricsObject, Integer> value, Context context) {
        log.info("Sink: [{}] {} : {}",
                context.timestamp() - lastTs, value.f0.getClientIp(), value.f1);
    }
}
