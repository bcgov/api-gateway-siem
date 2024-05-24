package bcgov.aps.geoloc;

import bcgov.aps.JsonUtils;
import bcgov.aps.models.GeoLocInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
public class IPWhoVendor implements GeoLocService {
    static final String IPWHO_ENDPOINT = "https://ipwho.is";

    private final transient ObjectMapper objectMapper;

    public IPWhoVendor() {
        objectMapper = JsonUtils.getObjectMapper();
    }

    public GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException {
        String apiUrl = String.format("%s/%s", IPWHO_ENDPOINT, ip);

        HttpURLConnection connection =
                (HttpURLConnection) new URL(apiUrl).openConnection();
        connection.setConnectTimeout(1000);
        connection.setRequestMethod("GET");
        connection.connect();

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new IOException("Failed to fetch user " +
                    "details: HTTP " + responseCode);
        }

        GeoLocInfo geo =
                objectMapper.readValue(connection.getInputStream(), GeoLocInfo.class);
        log.info("[{}] {} {}", ip, geo.isSuccess() ? "OK" : "ER", geo.getCountry());

        if (!geo.isSuccess()) {
            throw new IOException("IPWho returned a failed response.");
        }
        return geo;
    }
}
