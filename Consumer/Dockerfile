FROM bitnami/spark:latest

WORKDIR /app
COPY ../yelp_dataset/ .

ENV DATASET_PATH=/app/yelp_dataset/

COPY target/scala-2.12/consumer_2.12-0.1.jar /app/producer.jar

CMD ["/opt/bitnami/spark/bin/spark-submit", "--class", "MainClassName", "/app/producer.jar"]
