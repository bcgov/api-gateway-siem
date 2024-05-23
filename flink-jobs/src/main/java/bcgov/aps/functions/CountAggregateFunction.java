package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class CountAggregateFunction implements AggregateFunction<Tuple2<KongLogRecord, Integer>, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple2<KongLogRecord, Integer> value, Integer accumulator) {
        log.debug("Aggregate.add {} to {} ", value.f1
                , accumulator);
        return accumulator + value.f1;
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
