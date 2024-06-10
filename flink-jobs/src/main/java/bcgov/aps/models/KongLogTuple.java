package bcgov.aps.models;

import lombok.*;

import java.util.Set;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class KongLogTuple {
    KongLogRecord kongLogRecord;
    Integer value;
}
