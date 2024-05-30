package bcgov.aps.functions;

import bcgov.aps.models.MetricsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import java.util.HashMap;
import java.util.Map;


@Slf4j
public class FlinkMetricsExposingMapFunction extends RichMapFunction<Tuple2<MetricsObject, Integer>, Tuple2<MetricsObject, Integer>> {
    private static final long serialVersionUID = 1L;

    private transient OperatorMetricGroup parentMetricGroup;

    private transient MetricGroup metricGroup;

    private transient Long lastMetricTs = Long.valueOf(0L);

    private Map<String, MetricGroup> ips;

    private transient int valueToExpose;
    private transient Counter customCounter1;

    @Override
    public void open(Configuration parameters) {
        log.debug("Prometheus Map - OPEN");

        ips = new HashMap<>();

        parentMetricGroup = getRuntimeContext().getMetricGroup();

        parentMetricGroup
                .gauge("aps_siem_counter", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return valueToExpose;
                    }
                });

        parentMetricGroup
                .gauge("aps_siem_ip_buffer", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return ips.size();
                    }
                });

    }

    @Override
    public Tuple2<MetricsObject, Integer> map(Tuple2<MetricsObject, Integer> value) {
        log.debug("Prometheus Map");
        valueToExpose++;

        log.debug("ExposeMap {} {}", value, lastMetricTs);
        if (value == null || value.f0 == null || lastMetricTs == null) {
            lastMetricTs = value.f0.getWindowTime();
            metricGroup = parentMetricGroup.addGroup("ts", lastMetricTs.toString());
        } else if (value.f0.getWindowTime() == lastMetricTs) {
        } else if (value.f0.getWindowTime() > lastMetricTs) {
            lastMetricTs = value.f0.getWindowTime();
            log.debug("Clearing {}", ips.size());
//            ips.forEach((key, m) -> {
//                AbstractMetricGroup grp = (AbstractMetricGroup) m;
//                grp.close();
//            });
            ((AbstractMetricGroup)metricGroup).close();
            metricGroup = parentMetricGroup.addGroup("ts", lastMetricTs.toString());
            ips.clear();
        }
        setIpTopNGauge(value);
        return value;
    }

    private void setIpTopNGauge (Tuple2<MetricsObject, Integer> value) {
        String cacheKey = value.f0.getCacheKey();
        MetricGroup grp = ips.get(cacheKey);
        if (grp == null) {
            grp = metricGroup
                    .addGroup("client_ip", value.f0.getClientIp())
                    .addGroup("geo_conn_isp", value.f0.getGeo().getConnection().getIsp())
                    .addGroup("geo_conn_org", value.f0.getGeo().getConnection().getOrg())
                    .addGroup("geo_country", value.f0.getGeo().getCountry())
                    .addGroup("http_status", value.f0.getStatus().name())
                    .addGroup("namespace", value.f0.getNamespace())
                    .addGroup("request_uri_host", value.f0.getRequestUriHost());
            ips.put(cacheKey, grp);
        }

        log.debug("aps_siem_ip_topn {}", value);
        grp.gauge("aps_siem_ip_topn", () -> value.f1);
    }
}