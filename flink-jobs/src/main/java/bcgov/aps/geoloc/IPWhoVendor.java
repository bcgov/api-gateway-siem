package bcgov.aps.geoloc;

import bcgov.aps.JsonUtils;
import bcgov.aps.models.GeoLocInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
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
        log.info("[IPWhoVendor] {}", ip);
        String apiUrl = String.format("%s/%s",
                IPWHO_ENDPOINT, ip);

        HttpURLConnection connection =
                (HttpURLConnection) new URL(apiUrl).openConnection();
        connection.setConnectTimeout(1000);
        connection.setRequestMethod("GET");
        connection.connect();

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            log.error("Failed response {} {}",
                    responseCode,
                    connection.getResponseMessage());
            throw new IOException("Failed to fetch geo " +
                    "loc " +
                    "details: HTTP " + responseCode);
        }

        GeoLocInfo geo =
                objectMapper.readValue(connection.getInputStream(), GeoLocInfo.class);
        log.info("[{}] {} {}", ip, geo.isSuccess() ?
                "OK" : "ER", geo.getCountry());

        if (!geo.isSuccess()) {
            log.error("[{}] {} {}", ip, geo.getMessage());
            throw new IOException("IPWho returned a " +
                    "failed response.");
        }
        return geo;
    }

    @Override
    public void prefetch(LoadingCache<String, GeoLocInfo> ips) {

    }
}
