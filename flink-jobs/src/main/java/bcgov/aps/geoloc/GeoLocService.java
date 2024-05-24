package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;

import java.io.IOException;

public interface GeoLocService {
    GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException;
}
