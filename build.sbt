name := "SparkStreamingProject"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0", // <--- cette ligne pour Kafka
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.postgresql" % "postgresql" % "42.7.1"
  )
