package bcgov.aps.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeoLocInfo {
    boolean success;
    String country;
    String region;

    String city;

    @Getter
    @Setter
    static public class Connection {
        String org;
        String isp;
        String domain;
    }

    Connection connection;
}
