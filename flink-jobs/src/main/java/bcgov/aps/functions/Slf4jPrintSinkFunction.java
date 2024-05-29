package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Date;

@Slf4j
public class Slf4jPrintSinkFunction implements SinkFunction<Tuple2<MetricsObject, Integer>> {

    private long lastTs;
    private long sinkTs;

    private long counter;

    private long sinkTotal;


    public Slf4jPrintSinkFunction() {
        super();
        lastTs = new Date().getTime();
    }

    @Override
    public void invoke(Tuple2<MetricsObject, Integer> value, Context context) {
        counter += value.f1;
        if (context.timestamp() != sinkTs) {
            sinkTotal = 0;
            sinkTs = context.timestamp();
        }
        sinkTotal += value.f1;
        log.info("Sink: [{}] {} {} {} : {}",
                context.timestamp() - lastTs, sinkTotal, counter, value.f0.getClientIp(), value.f1);
    }
}
