package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LoadingMemoryCache implements GeoLocService {
    private final LoadingCache<String, GeoLocInfo> ips;

    public LoadingMemoryCache(GeoLocService geoLocService) {
        ips = CacheBuilder.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String,
                                GeoLocInfo>() {
                            @Override
                            public GeoLocInfo load(String ip) throws IOException, ExecutionException {
                                return geoLocService.fetchGeoLocationInformation(ip);
                            }
                        });
        geoLocService.prefetch(ips);
    }

    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException, ExecutionException {
        log.info("[LoadingMemoryCache] ({}) {}", ips.size(), ip);
        return ips.get(ip);
    }

    @Override
    public void prefetch(LoadingCache<String, GeoLocInfo> ips) {
    }

}
