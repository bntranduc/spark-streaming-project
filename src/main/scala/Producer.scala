import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import Config._


object Producer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    // Producer for Business data
    sendJsonToKafka(spark, BUSINESS_JSON_PATH, BUSINESS_SCHEMA, BUSINESS_TOPIC, BOOTSTRAP_SERVER)

    // Producer for Review data
    // sendJsonToKafka(spark, REVIEW_JSON_PATH, REVIEW_SCHEMA, REVIEW_TOPIC, BOOTSTRAP_SERVER)

    // Producer for User data
    // sendJsonToKafka(spark, USER_JSON_PATH, USER_SCHEMA, USER_TOPIC, BOOTSTRAP_SERVER)

    // Producer for Tip data
    // sendJsonToKafka(spark, TIP_JSON_PATH, TIP_SCHEMA, TIP_TOPIC, BOOTSTRAP_SERVER)

    spark.stop()
  }

  def sendJsonToKafka(spark: SparkSession, path: String, schema: StructType, topic: String, bootstrapServers: String): Unit = {
    println(s"\n========================================== TOPIC : $topic ==========================================\n")
    val df = spark.read.schema(schema).json(path)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val rows = df.toLocalIterator()

    var index = 0

    while (rows.hasNext) {
      val row = rows.next()
      val rowMap = row.getValuesMap[Any](row.schema.fieldNames)
      val jsonString = mapper.writeValueAsString(rowMap)

      val record = new ProducerRecord[String, String](topic, s"record_$index", jsonString)
      producer.send(record)

      println(s"record_$index sent")
      index += 1

      Thread.sleep(500)
    }

    producer.close()
  }
}
