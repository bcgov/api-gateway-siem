package bcgov.aps.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class KongLogTuple {
    KongLogRecord kongLogRecord;
    Integer value;
}
