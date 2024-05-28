package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class CountLogTupleAggregateFunction implements AggregateFunction<KongLogTuple, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(KongLogTuple value, Integer accumulator) {
        log.debug("Aggregate.add {} to {} ", value.getKongLogRecord()
                , accumulator);
        return accumulator + value.getValue();
    }

    @Override
    public Integer getResult(Integer accumulator) {
        log.debug("Aggregate.getResult {}",
                accumulator);
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        log.debug("Aggregate.merge {} {}", a, b);
        return a + b;
    }
}
