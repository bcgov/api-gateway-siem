package bcgov.aps.functions;

import bcgov.aps.JsonUtils;
import bcgov.aps.models.KongLogRecord;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class JsonParserProcessFunction extends ProcessFunction<String, Tuple2<KongLogRecord, Integer>> {

    private final ObjectMapper objectMapper;

    public JsonParserProcessFunction() {
        objectMapper = JsonUtils.getObjectMapper();
    }

    @Override
    public void processElement(String value,
                               Context ctx,
                               Collector<Tuple2<KongLogRecord, Integer>> out) {
        try {
            KongLogRecord jsonNode =
                    objectMapper.readValue(value,
                            KongLogRecord.class);
            log.debug("{}",
                    jsonNode);
            out.collect(new Tuple2<>(jsonNode, 1));
        } catch (Exception e) {
            log.error("Parsing error", e);
        }
    }
}
