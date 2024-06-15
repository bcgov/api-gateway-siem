package bcgov.aps.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KongLogRecord {
    @JsonProperty(value = "authenticated_entity")
    AuthenticatedEntity authenticatedEntity;
    @JsonProperty(value = "client_ip")
    String clientIp;
    @JsonProperty(value = "request_uri_host")
    String requestUriHost;
    @JsonProperty(value = "started_at")
    Long timestamp;
    GWRequest request;
    GWResponse response;
    GWLatencies latencies;
    String namespace;

    @JsonIgnore
    Map<String, Tuple2<String, Integer>> stash;

}
