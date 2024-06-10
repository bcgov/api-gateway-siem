package bcgov.aps.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GeoLocInfo {
    String ip;
    boolean success;
    String message;
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

    static public GeoLocInfo newEmptyGeoInfo(String ip) {
        GeoLocInfo geo = new GeoLocInfo();
        geo.setIp(ip);
        geo.setSuccess(false);
        geo.setCountry("-");
        geo.setRegion("-");
        geo.setConnection(new GeoLocInfo.Connection());
        geo.getConnection().setIsp("-");
        geo.getConnection().setOrg("-");
        return geo;
    }
}
