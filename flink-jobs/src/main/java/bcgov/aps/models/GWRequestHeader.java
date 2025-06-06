package bcgov.aps.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GWRequestHeader {

    @JsonProperty("host")
    String host;

    @JsonProperty("x-auth-jti")
    String authJti;

    @JsonProperty("x-auth-sub")
    String authSub;
}
