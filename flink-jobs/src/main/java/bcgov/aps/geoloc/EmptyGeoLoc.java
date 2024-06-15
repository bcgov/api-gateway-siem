package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;
import com.google.common.cache.LoadingCache;

public class EmptyGeoLoc implements GeoLocService {
    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) {
        return GeoLocInfo.newEmptyGeoInfo(ip);
    }

    @Override
    public void prefetch(LoadingCache<String, GeoLocInfo> ips) {

    }
}
