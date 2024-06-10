package bcgov.aps.functions;

import bcgov.aps.models.AuthSubWindowKey;
import bcgov.aps.models.KongLogTuple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class AuthFlattenMapFunction implements FlatMapFunction<KongLogTuple, Tuple2<String, Integer>> {
    static final String DELIMITER = "#";

    @Override
    public void flatMap(KongLogTuple log, Collector<Tuple2<String, Integer>> out) throws Exception {
        int count = log.getKongLogRecord().getClientIp().split(DELIMITER).length;
        if (count > 1) {
            out.collect(new Tuple2<>(AuthSubWindowKey.getKey(log.getKongLogRecord()), log.getValue()));
        }
    }
}
