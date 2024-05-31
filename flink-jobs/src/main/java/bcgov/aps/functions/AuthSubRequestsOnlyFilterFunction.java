package bcgov.aps.functions;

import bcgov.aps.models.KongLogTuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;

@Slf4j
public class AuthSubRequestsOnlyFilterFunction implements FilterFunction<KongLogTuple> {
    @Override
    public boolean filter(KongLogTuple kong) throws Exception {
        return kong.getKongLogRecord().getRequest().getHeaders().getAuthSub() != null;
    }
}
