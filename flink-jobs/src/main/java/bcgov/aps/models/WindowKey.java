package bcgov.aps.models;

public class WindowKey {
    static public String getKey(KongLogRecord rec) {
        boolean isRequestSuccess = rec.response.status >= 200 && rec.response.status < 400;
        return String.format("%s,%s,%s,%s", rec.namespace, rec.requestUriHost, rec.clientIp, isRequestSuccess);
    }


    static public MetricsObject parseKey(String key) {
        String[] parts = key.split(",");
        MetricsObject record = new MetricsObject();
        record.setNamespace(parts[0]);
        record.setRequestUriHost(parts[1]);
        record.setClientIp(parts[2]);
        record.setStatus(Boolean.parseBoolean(parts[3]) ? MetricsObject.HTTP_STATUS.OK: MetricsObject.HTTP_STATUS.Error);
        record.setCacheKey(key);
        return record;
    }
}
