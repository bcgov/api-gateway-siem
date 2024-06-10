package bcgov.aps.functions;

import bcgov.aps.JsonUtils;
import bcgov.aps.models.GeoLocInfo;
import bcgov.aps.models.KongLogRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class JsonGeoParserFunction extends ProcessFunction<String, GeoLocInfo> {

    private final ObjectMapper objectMapper;

    public JsonGeoParserFunction() {
        objectMapper = JsonUtils.getObjectMapper();
    }

    @Override
    public void processElement(String value,
                               Context ctx,
                               Collector<GeoLocInfo> out) {
        try {
            GeoLocInfo jsonNode =
                    objectMapper.readValue(value,
                            GeoLocInfo.class);

            out.collect(jsonNode);
        } catch (Exception e) {
            log.error("Parsing error", e);
        }
    }
}
