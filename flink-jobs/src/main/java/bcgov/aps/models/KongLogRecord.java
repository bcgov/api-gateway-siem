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
public class KongLogRecord {
    @JsonProperty(value = "client_ip")
    String clientIp;
    @JsonProperty(value = "request_uri_host")
    String requestUriHost;
    @JsonProperty(value = "started_at")
    Long timestamp;
    @JsonProperty(value = "rwquest")
    GWResponse request;
    @JsonProperty(value = "response")
    GWResponse response;
    String namespace;
}
