package bcgov.aps.models;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Base64;

/**
 * OIDC plugin has this:
 * "authenticated_entity": {
 * "id": "1ddaaf63-9da3-4711-8b54-f2437f8b6d43"
 * }
 * <p>
 * JWT-KEYCLOAK does not have anything
 */

@Slf4j
public class AuthSubWindowKey {
    static public String getKey(KongLogRecord rec) {
        return String.format("%s,%s,%s,%s,%s,%s",
                rec.getClientIp(),
                rec.getRequestUriHost(),
                rec.getRequest().getHeaders().getAuthSub(),
                rec.getAuthenticatedEntity() == null ?
                        null :
                        rec.getAuthenticatedEntity().getId(),
                rec.getConsumer() == null ? null :
                        rec.getConsumer().getUsername(),
                rec.getConsumerTags() == null || rec.getConsumerTags().length() == 0 ? null :
                        Base64.getEncoder().encodeToString(rec.getConsumerTags().getBytes()));
    }

    static public MetricsObject parseKey(String key) {
        String[] parts = key.split(",");
        MetricsObject record = new MetricsObject();
        record.setClientIp(parts[0]);
        record.setRequestUriHost(parts[1]);
        if (!parts[2].equals("null")) {
            record.setAuthSub(parts[2]);
            record.setAuthType(MetricsObject.AUTH_TYPE.jwt);
        } else if (!parts[3].equals("null")) {
            record.setAuthSub(parts[3]);
            record.setAuthType(MetricsObject.AUTH_TYPE.oidc);
        }
        if (!parts[4].equals("null")) {
            record.setConsumerUsername(parts[4]);
        }
        if (parts.length == 5) {
            log.warn("MISSING_PART_OF_KEY {}", key);
        } else if (!parts[5].equals("null")) {
            record.setConsumerTags(new String(Base64.getDecoder().decode(parts[5])));
        }
        record.setSegments(new Segments());
        return record;
    }

    static public Tuple2<String, MetricsObject.AUTH_TYPE> getAuthSub(KongLogRecord rec) {
        if (rec.getRequest().getHeaders().getAuthSub() == null) {
            return rec.getAuthenticatedEntity() == null ?
                    null :
                    new Tuple2(rec.getAuthenticatedEntity().getId(), MetricsObject.AUTH_TYPE.oidc);
        } else {
            return new Tuple2(rec.getRequest().getHeaders().getAuthSub(), MetricsObject.AUTH_TYPE.jwt);
        }
    }
}
