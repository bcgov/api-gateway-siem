package bcgov.aps.functions;

import bcgov.aps.JsonUtils;
import bcgov.aps.KafkaFlinkTopIP;
import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class SplitProcessFunction extends ProcessFunction<Tuple2<KongLogRecord, Integer>, Tuple2<KongLogRecord, Integer>> {

    private final OutputTag<KongLogTuple> side;


    public SplitProcessFunction(OutputTag<KongLogTuple> side) {
        this.side = side;
    }

    @Override
    public void processElement(Tuple2<KongLogRecord, Integer> value,
                               Context ctx,
                               Collector<Tuple2<KongLogRecord, Integer>> out) {
        ctx.output(side, new KongLogTuple(value.f0, value.f1));
        out.collect(value);
    }
}
