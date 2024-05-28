package bcgov.aps.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricsObject {
    public enum HTTP_STATUS { OK, Error, RateLimited, NA };
    public enum AUTH_TYPE { oidc, jwt }

    String namespace;

    @JsonProperty(value = "client_ip")
    String clientIp;

    @JsonProperty(value = "request_uri_host")
    String requestUriHost;

    @JsonProperty(value = "window_ts")
    Long windowTime;

    @JsonProperty(value = "success")
    HTTP_STATUS status;

    @JsonProperty(value = "auth_hash")
    String authHash;

    @JsonProperty(value = "auth_type")
    AUTH_TYPE authType;


    GeoLocInfo geo;
}
