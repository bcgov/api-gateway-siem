package bcgov.aps.geoloc;

import bcgov.aps.models.GeoLocInfo;

import java.io.IOException;

public class NullGeoLoc implements GeoLocService {
    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException {
        GeoLocInfo info = new GeoLocInfo();
        info.setConnection(new GeoLocInfo.Connection());
        return info;
    }
}
