package bcgov.aps.models;

import lombok.extern.slf4j.Slf4j;

/**
 * OIDC plugin has this:
 * "authenticated_entity": {
 * "id": "1ddaaf63-9da3-4711-8b54-f2437f8b6d43"
 * }
 * <p>
 * JWT-KEYCLOAK does not have anything
 */

public class AuthSubWindowKey {
    static public String getKey(KongLogRecord rec) {
        return String.format("%s,%s,%s",
                rec.getClientIp(),
                rec.getRequest().getHeaders().getAuthSub(),
                rec.getAuthenticatedEntity() == null ?
                        null :
                        rec.getAuthenticatedEntity().getId());
    }

    static public MetricsObject parseKey(String key) {
        String[] parts = key.split(",");
        MetricsObject record = new MetricsObject();
        record.setClientIp(parts[0]);
        if (!parts[1].equals("null")) {
            record.setAuthSub(parts[1]);
            record.setAuthType(MetricsObject.AUTH_TYPE.jwt);
        } else if (!parts[2].equals("null")) {
            record.setAuthSub(parts[2]);
            record.setAuthType(MetricsObject.AUTH_TYPE.oidc);
        }
        return record;
    }
}
