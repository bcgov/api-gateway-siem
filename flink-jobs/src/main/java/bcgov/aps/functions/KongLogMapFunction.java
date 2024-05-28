package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class KongLogMapFunction extends RichMapFunction<Tuple2<KongLogRecord, Integer>, KongLogTuple> {
    private static final long serialVersionUID = 1L;

    @Override
    public KongLogTuple map(Tuple2<KongLogRecord, Integer> value) {
        return new KongLogTuple(value.f0, value.f1);
    }
}