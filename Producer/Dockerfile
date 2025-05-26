# Étape 1 : Compilation
FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.22_7_1.9.9_2.12.19 AS builder

WORKDIR /app
COPY . .
RUN sbt clean assembly

# Étape 2 : Exécution
FROM openjdk:11.0.11-jre-slim

RUN apt-get update && \
    apt-get install -y curl && \
    curl -fLo spark.tgz https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz && \
    tar -xzf spark.tgz && \
    mv spark-3.5.5-bin-hadoop3 /opt/spark && \
    rm spark.tgz && \
    apt-get clean

WORKDIR /app
COPY --from=builder /app/target/scala-2.12/app.jar .

CMD ["spark-submit", "--class", "Main", "--master", "local[*]", "app.jar"]
