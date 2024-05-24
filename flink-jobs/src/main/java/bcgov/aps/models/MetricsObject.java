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
    @JsonProperty(value = "client_ip")
    String clientIp;

    @JsonProperty(value = "request_uri_host")
    String requestUriHost;

    @JsonProperty(value = "window_ts")
    Long windowTime;

    @JsonProperty(value = "success")
    boolean success;

    GeoLocInfo geo;
}
