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
    private final OutputTag<KongLogTuple> side2;
    private final OutputTag<KongLogTuple> side3;

    public SplitProcessFunction(OutputTag<KongLogTuple> side, OutputTag<KongLogTuple> side2, OutputTag<KongLogTuple> side3) {
        this.side = side;
        this.side2 = side2;
        this.side3 = side3;
    }

    @Override
    public void processElement(Tuple2<KongLogRecord, Integer> value,
                               Context ctx,
                               Collector<Tuple2<KongLogRecord, Integer>> out) {
        ctx.output(side, new KongLogTuple(value.f0, value.f1));
        ctx.output(side2, new KongLogTuple(value.f0, value.f1));
        ctx.output(side3, new KongLogTuple(value.f0, value.f1));
        out.collect(value);
    }
}
