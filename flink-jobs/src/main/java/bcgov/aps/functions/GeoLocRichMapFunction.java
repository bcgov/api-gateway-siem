package bcgov.aps.functions;

import bcgov.aps.geoloc.GeoLocService;
import bcgov.aps.geoloc.IPWhoVendor;
import bcgov.aps.geoloc.KafkaTopicCache;
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


    @Override
    public void open(Configuration parameters) {
        geoLocService = GeoLocService.factory(GeoLocService.VENDOR.IPWHO);
    }

    @Override
    public Tuple2<MetricsObject, Integer> map(Tuple2<MetricsObject, Integer> value) {
        String ip = value.f0.getClientIp();
        if (ip == null ||
                ip.equals("other")) {
            GeoLocInfo loc = GeoLocInfo.newEmptyGeoInfo(ip);
            loc.setCountry("MissingIP");
            value.f0.setGeo(loc);
            return value;
        }
        try {
            value.f0.setGeo(geoLocService.fetchGeoLocationInformation(ip));
        } catch (ExecutionException e) {
            log.error("Execution Exception {}", e.getMessage());
            GeoLocInfo loc = GeoLocInfo.newEmptyGeoInfo(ip);
            loc.setCountry("Err");
            value.f0.setGeo(loc);
        } catch (IOException e) {
            log.error("IO Exception {}", e.getMessage());
            GeoLocInfo loc = GeoLocInfo.newEmptyGeoInfo(ip);
            loc.setCountry("Err");
            value.f0.setGeo(loc);
        }
        return value;
    }
}
