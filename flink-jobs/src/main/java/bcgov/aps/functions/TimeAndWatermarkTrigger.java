package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

@Slf4j
public class TimeAndWatermarkTrigger extends Trigger<Tuple2<KongLogRecord, Integer>, TimeWindow> {

    private final long processingTimeInterval;

    public TimeAndWatermarkTrigger(long processingTimeInterval) {
        this.processingTimeInterval = processingTimeInterval;
    }

    @Override
    public TriggerResult onElement(Tuple2<KongLogRecord, Integer> value, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // Register a processing time timer if it hasn't been registered yet
        if (ctx.getCurrentProcessingTime() < window.maxTimestamp()) {
            ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + processingTimeInterval);
        }

        log.info("[onElement] {} {} {}", value.f0.getRequestUriHost(), timestamp, window.maxTimestamp());

        // Register an event time timer if it hasn't been registered yet
        if (timestamp <= window.maxTimestamp()) {
            log.info("[onElement] Register Event Time Timer");
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        log.info("[onEventTime] {} {}", time, window.maxTimestamp());
        if (time == window.maxTimestamp()) {
            // Fire the window when the watermark passes the end of the window
            log.info("[onEventTime] {} FIRE", time);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        // Fire the window on processing time timer
        if (time >= window.maxTimestamp()) {
            log.info("[onProcessingTime] {} FIRE", time);
            return TriggerResult.FIRE;
        } else {
            log.info("[onProcessingTime] {} REREGISTER", time);
            // Re-register the processing time timer
            ctx.registerProcessingTimeTimer(time + processingTimeInterval);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // Clear any registered timers
        log.info("[clear] {}", window.maxTimestamp());
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.deleteProcessingTimeTimer(ctx.getCurrentProcessingTime() + processingTimeInterval);
    }

    public static TimeAndWatermarkTrigger of(long processingTimeInterval) {
        return new TimeAndWatermarkTrigger(processingTimeInterval);
    }
}

