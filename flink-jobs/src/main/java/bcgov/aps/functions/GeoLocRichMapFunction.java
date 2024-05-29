package bcgov.aps.functions;

import bcgov.aps.geoloc.GeoLocService;
import bcgov.aps.geoloc.IPWhoVendor;
import bcgov.aps.geoloc.NullGeoLoc;
import bcgov.aps.models.GeoLocInfo;
import bcgov.aps.models.MetricsObject;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GeoLocRichMapFunction extends RichMapFunction<Tuple2<MetricsObject, Integer>, Tuple2<MetricsObject, Integer>> {

    private transient GeoLocService geoLocService;

    private transient LoadingCache<String, GeoLocInfo> ips;

    @Override
    public void open(Configuration parameters) {
        geoLocService = new IPWhoVendor();
        ips = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, GeoLocInfo>() {
                            @Override
                            public GeoLocInfo load(String ip) throws IOException {
                                return geoLocService.fetchGeoLocationInformation(ip);
                            }
                        });
    }

    @Override
    public Tuple2<MetricsObject, Integer> map(Tuple2<MetricsObject, Integer> value) {
        if (value.f0.getClientIp() == null ||
                value.f0.getClientIp().equals("other")) {
            return value;
        }
        try {
            value.f0.setGeo(ips.get(value.f0.getClientIp()));
        } catch (ExecutionException e) {
            log.error("Execution Exception {}", e.getMessage());
        }
        return value;
    }


}
