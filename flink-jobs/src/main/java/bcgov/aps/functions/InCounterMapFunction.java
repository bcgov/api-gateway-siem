package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

@Slf4j
public class InCounterMapFunction extends RichMapFunction<Tuple2<KongLogRecord, Integer>, Tuple2<KongLogRecord, Integer>> {
    private static final long serialVersionUID = 1L;
    private transient int valueToExpose;
    @Override
    public void open(Configuration parameters) {

        getRuntimeContext().getMetricGroup()
                .gauge("aps_siem_req_counter",
                        new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        log.info("[aps_siem_req_counter] " +
                                "{} ", valueToExpose);
                        return valueToExpose;
                    }
                });
    }

    @Override
    public Tuple2<KongLogRecord, Integer> map(Tuple2<KongLogRecord, Integer> value) {
        valueToExpose++;
        return value;
    }
}