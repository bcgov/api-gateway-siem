package bcgov.aps.functions;

import bcgov.aps.models.GWRequest;
import bcgov.aps.models.GWRequestHeader;
import bcgov.aps.models.KongLogRecord;
import bcgov.aps.models.KongLogTuple;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.HashSet;
import java.util.Set;

public class AuthIPReduceFunction implements ReduceFunction<KongLogTuple> {
    static final String DELIMITER = "#";

    @Override
    public KongLogTuple reduce(KongLogTuple value1, KongLogTuple value2) {

        Set<String> ips = new HashSet<>();
        Set.of(value1.getKongLogRecord().getClientIp().split(DELIMITER)).stream().forEach(ips::add);
        Set.of(value2.getKongLogRecord().getClientIp().split(DELIMITER)).stream().forEach(ips::add);

        KongLogTuple t = new KongLogTuple(new KongLogRecord(), value1.getValue() + value2.getValue());
        t.getKongLogRecord().setClientIp(String.join(DELIMITER, ips));
        t.getKongLogRecord().setRequest(new GWRequest());
        t.getKongLogRecord().getRequest().setHeaders(new GWRequestHeader());
        t.getKongLogRecord().getRequest().getHeaders().setAuthSub(value1.getKongLogRecord().getRequest().getHeaders().getAuthSub());
        return t;
    }
}
