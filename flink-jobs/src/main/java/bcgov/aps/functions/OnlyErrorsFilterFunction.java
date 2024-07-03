package bcgov.aps.functions;

import bcgov.aps.models.KongLogTuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;

@Slf4j
public class OnlyErrorsFilterFunction implements FilterFunction<KongLogTuple> {
    @Override
    public boolean filter(KongLogTuple kong) throws Exception {
        return kong.getKongLogRecord().getResponse().getStatus() >= 200
                && kong.getKongLogRecord().getResponse().getStatus() < 400;
    }
}
