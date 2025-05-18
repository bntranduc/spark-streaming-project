FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

WORKDIR /app
COPY yelp_dataset/ /yelp_dataset

ENV DATASET_PATH=/yelp_dataset/

COPY target/scala-2.12/consumer_2.12-0.1.jar /app/consumer.jar

CMD ["/opt/spark/bin/spark-submit", "--class", "Consumer", "/app/consumer.jar"]
