# Kong Gateway SIEM by API Programme Services

## Getting Started

```sh
docker build --tag aps-flink-jobs flink-jobs

docker run -ti --rm --name flink \
 -p 8081:8081 \
 -p 9251:9251 \
 -e "KAFKA_BOOTSTRAP_SERVERS=kafka:9092" \
 -e "KAFKA_TOPICS=default" \
  aps-flink-jobs
```

**Endpoints:**

- Flink UI: http://localhost:8081
- Prometheus: http://localhost:9251/metrics

**Metrics:**

| Metric                                                     | Description                               |
| ---------------------------------------------------------- | ----------------------------------------- |
| flink_taskmanager_job_task_operator_ts_ip_aps_siem_ip_topn | Gauge of Top N IPs                        |
| flink_taskmanager_job_task_operator_aps_siem_ip            | Gauge of total unique IPs in 30s interval |
| flink_taskmanager_job_task_operator_aps_siem_requests      | Gauge of request count in 30s interval    |
| flink_taskmanager_job_task_operator_aps_siem_ip_buffer     | Gauge of IPs data buffered                |
| flink_taskmanager_job_task_operator_aps_siem_counter       | Gauge of total mappings                   |

## Use Cases

### Top Number of requests by IP

Identifies and monitors IP addresses generating the highest volume of requests to your API gateway. This helps detect potential DoS attacks, aggressive scrapers, or misconfigured clients that may be overwhelming your services.

### Top IPs with Unusual Number of Errors

Tracks IP addresses experiencing abnormally high error rates, which could indicate potential API abuse, brute force attempts, or clients targeting vulnerable endpoints. This metric helps identify both security threats and problematic API implementations.

### Credentials used from different IPs

Monitors when the same authentication credentials are used across multiple IP addresses within short time periods. This pattern often indicates credential theft, account sharing, or distributed attacks using compromised credentials.

### Some successful requests and then spike of failed requests

Detects when an IP address shows a pattern of successful API requests followed by a sudden increase in failures. This behavior is common in reconnaissance activities where attackers probe endpoints before launching targeted attacks.
