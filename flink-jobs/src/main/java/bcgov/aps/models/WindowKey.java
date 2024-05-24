package bcgov.aps.models;

public class WindowKey {
    static public String getKey(KongLogRecord rec) {
        boolean isRequestSuccess = rec.response.status >= 200 && rec.response.status < 400;
        return String.format("%s,%s,%s", rec.requestUriHost, rec.clientIp, isRequestSuccess);
    }


    static public MetricsObject parseKey(String key) {
        String[] parts = key.split(",");
        MetricsObject record = new MetricsObject();
        record.setRequestUriHost(parts[0]);
        record.setClientIp(parts[1]);
        record.setSuccess(Boolean.parseBoolean(parts[2]));
        return record;
    }
}
