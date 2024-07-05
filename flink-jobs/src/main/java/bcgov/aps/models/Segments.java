package bcgov.aps.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Segments {
    String phase; // non-prod or production / tech_preview, beta, general availability (GA)
    String ip;
}
