docker build -t consumer-app .
docker run consumer-app

# clean and recompile
sbt clean
sbt package

export SBT_OPTS="-Xmx4G -Xms2G"