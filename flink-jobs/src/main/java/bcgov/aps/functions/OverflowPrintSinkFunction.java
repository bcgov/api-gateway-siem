package bcgov.aps.functions;

import bcgov.aps.models.KongLogTuple;
import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Date;

@Slf4j
public class OverflowPrintSinkFunction implements SinkFunction<KongLogTuple> {

    private long lastTs;


    public OverflowPrintSinkFunction() {
        super();
        lastTs = new Date().getTime();
    }

    @Override
    public void invoke(KongLogTuple value, Context context) {
        log.error("OutOfRange: [{}] {}",
                context.timestamp() - lastTs, value);
    }
}
