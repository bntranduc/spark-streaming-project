import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files, Path}
import java.io.File
import scala.collection.mutable
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import Config._

object FileBatchConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File Batch Consumer")
      .master("local[*]")
      .getOrCreate()

    val topic = REVIEW_TOPIC
    val batchFolder = REVIEW_OUTPUT_PATH

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", BOOTSTRAP_SERVER)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)

    watchAndSendBatches(batchFolder, topic, producer)

    producer.close()
    spark.stop()
  }

  def watchAndSendBatches(batchFolder: String, topic: String, producer: KafkaProducer[String, String]): Unit = {
    val seenBatches = mutable.Set[String]()

    while (true) {
      val batchDir = new File(batchFolder)
      if (batchDir.exists() && batchDir.isDirectory) {
        val batchFiles = batchDir.listFiles().filter(_.isDirectory).map(_.getName).sorted

        batchFiles.foreach { batchName =>
          if (!seenBatches.contains(batchName)) {
            println(s"New batch detected: $batchName")

            val batchPath = Paths.get(batchFolder, batchName)
            val jsonFiles = batchPath.toFile.listFiles().filter(_.getName.endsWith(".json"))

            jsonFiles.foreach { file =>
              val content = scala.io.Source.fromFile(file).getLines().mkString
              val record = new ProducerRecord[String, String](topic, content)
              producer.send(record)
              println(s"Sent file ${file.getName} to Kafka topic $topic")
            }

            seenBatches += batchName
          }
        }
      }

      Thread.sleep(5000) // Check toutes les 5 secondes
    }
  }
}
