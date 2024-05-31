package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;

import java.io.IOException;

public interface GeoLocService {
    static enum VENDOR { IPWHO, NULL }

    GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException;

    static GeoLocService factory(VENDOR vendor) {
        if (vendor == VENDOR.IPWHO) {
            return new IPWhoVendor();
        } else {
            return new NullGeoLoc();
        }
    }
}
