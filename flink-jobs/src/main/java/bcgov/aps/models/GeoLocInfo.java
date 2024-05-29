package bcgov.aps.models;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class GeoLocInfo {
    boolean success;
    String country;
    String region;

    String city;

    @Getter
    @Setter
    @ToString
    static public class Connection {
        String org;
        String isp;
        String domain;
    }

    Connection connection;

    static public GeoLocInfo newEmptyGeoInfo() {
        GeoLocInfo geo = new GeoLocInfo();
        geo.setSuccess(false);
        geo.setCountry("-");
        geo.setRegion("-");
        geo.setConnection(new GeoLocInfo.Connection());
        geo.getConnection().setIsp("-");
        geo.getConnection().setOrg("-");
        return geo;
    }
}
