package bcgov.aps.models;

/**
 * OIDC plugin has this:
 * "authenticated_entity": {
 * "id": "1ddaaf63-9da3-4711-8b54-f2437f8b6d43"
 * }
 * <p>
 * JWT-KEYCLOAK does not have anything
 */

public class AuthWindowKey {
    static public String getKey(KongLogRecord rec) {
        String authHash = "1234";
        return String.format("%s,%s,%s",
                rec.getNamespace(),
                rec.getRequestUriHost(),
                rec.getRequest().getHeaders().getAuthHash());
    }

    static public MetricsObject parseKey(String key) {
        String[] parts = key.split(",");
        MetricsObject record = new MetricsObject();
        record.setNamespace(parts[0]);
        record.setRequestUriHost(parts[1]);
        record.setAuthHash(parts[2]);
        return record;
    }
}
