package bcgov.aps.functions;

import bcgov.aps.models.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
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


//        t.getKongLogRecord().setStash(new HashMap<>());
//        if (value1.getKongLogRecord().getStash() != null) {
//            t.getKongLogRecord().getStash().putAll(value1.getKongLogRecord().getStash());
//        } else {
//            t.getKongLogRecord().getStash().put(value1.getKongLogRecord().getClientIp(), new Tuple2(AuthSubWindowKey.getKey(value1.getKongLogRecord()), value1.getValue()));
//        }
//        if (value2.getKongLogRecord().getStash() != null) {
//            t.getKongLogRecord().getStash().putAll(value2.getKongLogRecord().getStash());
//        } else {
//            t.getKongLogRecord().getStash().put(value2.getKongLogRecord().getClientIp(), new Tuple2(AuthSubWindowKey.getKey(value2.getKongLogRecord()), value2.getValue()));
//        }

        t.getKongLogRecord().setRequest(new GWRequest());
        t.getKongLogRecord().setRequestUriHost(value1.getKongLogRecord().getRequestUriHost());
        t.getKongLogRecord().getRequest().setHeaders(new GWRequestHeader());
        t.getKongLogRecord().getRequest().getHeaders().setAuthSub(value1.getKongLogRecord().getRequest().getHeaders().getAuthSub());
        return t;
    }
}
