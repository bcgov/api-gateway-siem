package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface GeoLocService {
    static enum VENDOR { IPWHO, NULL }

    GeoLocInfo fetchGeoLocationInformation(String ip) throws ExecutionException, IOException;

    static GeoLocService factory(VENDOR vendor) {
        if (vendor == VENDOR.IPWHO) {
            return new LoadingMemoryCache(new IPWhoVendor());
        } else {
            return new NullGeoLoc();
        }
    }
}
