package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface GeoLocService {
    static GeoLocService factory(VENDOR vendor) {
        if (vendor == VENDOR.IPWHO) {
            return new LoadingMemoryCache(new IPWhoVendor());
        } else if (vendor == VENDOR.KAFKA_WITH_IPWHO) {
            return new LoadingMemoryCache(new KafkaTopicCache(new IPWhoVendor()));
        } else if (vendor == VENDOR.KAFKA_ONLY) {
            return new LoadingMemoryCache(new KafkaTopicCache(new NullGeoLoc()));
        } else if (vendor == VENDOR.EMPTY) {
            return new EmptyGeoLoc();
        } else {
            return new NullGeoLoc();
        }
    }

    GeoLocInfo fetchGeoLocationInformation(String ip) throws ExecutionException, IOException;

    void prefetch (LoadingCache<String, GeoLocInfo> ips);

    enum VENDOR {IPWHO, KAFKA_WITH_IPWHO,
        KAFKA_ONLY, EMPTY, NULL}
}
