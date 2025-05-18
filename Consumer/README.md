docker build -t consumer-app .
docker run consumer-app

# clean and recompile
sbt clean
sbt package