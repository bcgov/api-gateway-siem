FROM amazoncorretto:22-alpine3.19

RUN wget https://dlcdn.apache.org/flink/flink-1.19.0/flink-1.19.0-bin-scala_2.12.tgz \
 && tar -xf flink-1.19.0-bin-scala_2.12.tgz \
 && rm flink-1.19.0-bin-scala_2.12.tgz
ENV PATH=$PATH:/flink-1.19.0/bin

RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz \
 && tar -xf apache-maven-3.9.6-bin.tar.gz \
 && rm apache-maven-3.9.6-bin.tar.gz
ENV PATH=$PATH:/apache-maven-3.9.6/bin

RUN apk add --no-cache bash

WORKDIR /app

COPY pom.xml .
RUN mvn clean package

COPY src src
RUN mvn clean package

COPY logback.xml logback.xml

COPY entrypoint.sh .

ENV FLINK_HOME=/flink-1.19.0

# COPY log4j.properties /flink-1.19.0/conf/log4j.properties
#COPY log4j.properties /flink-1.19.0/conf/log4j-cli.properties
#COPY log4j.properties /flink-1.19.0/conf/log4j-console.properties
#COPY log4j.properties /flink-1.19.0/conf/log4j-session.properties

# echo "metrics.reporters: prom" >> "$FLINK_HOME/conf/config.yaml"; \
RUN echo "metrics.latency.interval: 1000" >> "$FLINK_HOME/conf/config.yaml"; \
    echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> "$FLINK_HOME/conf/config.yaml"; \
    echo "metrics.reporter.prom.port: 9250-9260" >> "$FLINK_HOME/conf/config.yaml";

RUN echo "rest.bind-address: 0.0.0.0" >> "$FLINK_HOME/conf/config.yaml"; \
    echo "rest.address: 0.0.0.0" >> "$FLINK_HOME/conf/config.yaml"; \
    echo "rest.bind-port: 8081" >> "$FLINK_HOME/conf/config.yaml";

ENTRYPOINT ["/app/entrypoint.sh"]