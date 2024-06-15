package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;
import com.google.common.cache.LoadingCache;

import java.io.IOException;

public class NullGeoLoc implements GeoLocService {
    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) {
        return null;
    }

    @Override
    public void prefetch(LoadingCache<String, GeoLocInfo> ips) {

    }
}
