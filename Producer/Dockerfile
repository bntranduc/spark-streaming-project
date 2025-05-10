FROM bitnami/spark:latest

# Créer le dossier de travail
WORKDIR /app
COPY yelp_dataset/ .

ENV DATASET_PATH=/app/yelp_dataset/

# Copier le fichier JAR généré par sbt package
COPY target/scala-2.12/producer_2.12-0.1.jar /app/producer.jar

# Exécuter l’application Spark
# Main class = Main (car Main.scala)
CMD ["/opt/bitnami/spark/bin/spark-submit", "--class", "Main", "/app/producer.jar"]
