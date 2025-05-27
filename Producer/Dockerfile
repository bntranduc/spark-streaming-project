# Étape 1 : Compilation
FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.22_7_1.9.9_2.12.19 AS builder

WORKDIR /app

# Étape 1.1 : Copier uniquement les fichiers de config SBT
COPY build.sbt .
COPY project ./project

# Étape 1.2 : Télécharger les dépendances
RUN sbt update

# Étape 1.3 : Copier le code source
COPY . .

# Étape 1.4 : Compiler et packager
RUN sbt clean assembly

# Étape 2 : Exécution
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

WORKDIR /app

COPY --from=builder /app/target/scala-2.12/app.jar .

CMD ["spark-submit", "--class", "Main", "--master", "local[*]", "app.jar"]
