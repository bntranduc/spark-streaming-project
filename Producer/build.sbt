name := "producer"

version := "0.1"

scalaVersion := "2.12.18"

// enablePlugins(AssemblyPlugin)

val sparkVersion = "3.5.1"
val kafkaVersion = "3.6.1"
val mysqlVersion = "8.0.33"
val postgresVersion = "42.7.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)

Compile / compile / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "utf8"
)
