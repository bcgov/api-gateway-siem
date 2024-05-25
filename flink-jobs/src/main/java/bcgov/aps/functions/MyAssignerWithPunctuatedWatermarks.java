package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<Tuple2<KongLogRecord, Integer>> {
    private static final long serialVersionUID =
            -4834111073247835189L;
    private final long maxTimeLag = 10 * 1000L;

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<KongLogRecord, Integer> lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - maxTimeLag);
    }

    @Override
    public long extractTimestamp(Tuple2<KongLogRecord,
            Integer> element, long previousElementTimestamp) {
        long ts = element.f0.getTimestamp();
        return (ts);
    }
}
