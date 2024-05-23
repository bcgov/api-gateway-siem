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

### Top IPs with Unsual Number of Errors

### Credentials used from different IPs

### Some successful requests and then spike of failed requests
