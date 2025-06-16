# Étape 1 : Build
FROM hseeberger/scala-sbt:11.0.19_1.8.2_2.12.15 AS builder

WORKDIR /app

# Copier tout le projet
COPY . .

# Compiler et créer le JAR
RUN sbt clean assembly

# Étape 2 : Image finale
FROM openjdk:11-jre-slim

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Installer Spark
RUN apt-get update && apt-get install -y curl && \
    curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar xz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    apt-get clean

WORKDIR /app

# Copier le JAR compilé
COPY --from=builder /app/target/scala-2.12/app.jar .

# Lancer Spark
CMD ["spark-submit", "--class", "Main", "--master", "local[*]", "app.jar"]
