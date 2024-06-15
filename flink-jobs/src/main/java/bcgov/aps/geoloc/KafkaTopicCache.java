package bcgov.aps.geoloc;

import bcgov.aps.Config;
import bcgov.aps.JsonUtils;
import bcgov.aps.models.GeoLocInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class KafkaTopicCache implements GeoLocService {
    static final String TOPIC = "siem-ip";

    transient final ObjectMapper mapper =
            JsonUtils.getObjectMapper();

    private final transient GeoLocService geoLocService;

    private final transient KafkaProducer<String, String> producer;

    public KafkaTopicCache(GeoLocService geoLocService) {
        this.geoLocService = geoLocService;
        this.producer = prepareProducer();
    }

    @Override
    public GeoLocInfo fetchGeoLocationInformation(String ip) throws IOException, ExecutionException {
        GeoLocInfo geo = geoLocService.fetchGeoLocationInformation(ip);
        if (geo != null) {
            send(geo);
        } else {
            log.error("Failed to fetch geo " +
                    "loc " +
                    "details for {}", ip);
            return GeoLocInfo.newEmptyGeoInfo(ip);
        }
        return geo;
    }

    @Override
    public void prefetch(LoadingCache<String, GeoLocInfo> ips) {
        KafkaConsumerRunner runner =
                new KafkaConsumerRunner(ips);

        // get a reference to the current thread
        final Thread mainThread =
                Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's " +
                        "exit" +
                        " by calling shutdown" +
                        "()...");
                runner.shutdown();

                // join the main thread to allow the
                // execution of the code in the main
                // thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Interrupted after " +
                            "consumer" +
                            " shutdown", e);
                }
            }
        });

        ExecutorService executorService =
                Executors.newSingleThreadExecutor();

        CompletableFuture.runAsync(runner, executorService);
    }

    private KafkaProducer<String, String> prepareProducer() {
        Config config = new Config();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private void send(GeoLocInfo geo) throws JsonProcessingException {

        String value = mapper.writeValueAsString(geo);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, geo.getIp(),
                        value);
        producer.send(record);
    }
}
