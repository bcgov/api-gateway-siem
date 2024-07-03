package bcgov.aps.functions;

import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class AuthSubRequestsOnlyFilterFunction implements FilterFunction<KongLogTuple> {
    @Override
    public boolean filter(KongLogTuple kong) {
        log.debug("[OnlyErrors] {} {}", kong.getKongLogRecord(), kong.getKongLogRecord().getRequest().getHeaders().getAuthSub() != null);
        return kong.getKongLogRecord().getRequest().getHeaders().getAuthSub() != null;
    }
}
