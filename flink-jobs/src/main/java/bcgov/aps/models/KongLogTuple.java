package bcgov.aps.models;

import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class KongLogTuple {
    KongLogRecord kongLogRecord;
    Integer value;
}
