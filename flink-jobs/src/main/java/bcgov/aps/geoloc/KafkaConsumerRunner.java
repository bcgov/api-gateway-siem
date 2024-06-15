package bcgov.aps.geoloc;

import bcgov.aps.Config;
import bcgov.aps.JsonUtils;
import bcgov.aps.models.GeoLocInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Slf4j
public class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed =
            new AtomicBoolean(false);
    private final KafkaConsumer<String, String> consumer;

    private final LoadingCache<String, GeoLocInfo> cache;

    public KafkaConsumerRunner(LoadingCache<String, GeoLocInfo> kafkaCache) {
        Config config = new Config();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer =
                new KafkaConsumer<>(properties);

        cache = kafkaCache;
    }

    public void run() {
        final ObjectMapper mapper =
                JsonUtils.getObjectMapper();

        try {
            consumer.subscribe(Arrays.asList("siem-ip"));

            // Wait until the consumer is assigned partitions
            Set<TopicPartition> assignment;
            while ((assignment = consumer.assignment()).isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }

            // Seek to offset 0 for all assigned partitions
            for (TopicPartition partition : assignment) {
                consumer.seek(partition, 0);
            }

            while (!closed.get()) {
                ConsumerRecords<String,String> records =
                        consumer.poll(Duration.ofMillis(10000));
                log.info("[KafkaConsumerRunner] Caching {} records", records.count());

                for (ConsumerRecord<String,
                        String> record : records) {
                    try {
                        GeoLocInfo geo = mapper.readValue(record.value(), GeoLocInfo.class);
                        cache.put(geo.getIp(), geo);
                    } catch (JsonMappingException e) {
                        log.error("[{}] Catch GeoLocInfo json mapping error", record.key());
                    } catch (JsonProcessingException e) {
                        log.error("[{}] Catch GeoLocInfo json processing error", record.key());
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
            log.info(
                    "The consumer is now " +
                            "gracefully " +
                            "closed.");
        }
    }

    // Shutdown hook which can be called from a separate
    // thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
