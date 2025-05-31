import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import Config._


object Producer {

  def main(args: Array[String]): Unit = {

    // === Paramètres ===
    val inputPath = REVIEW_JSON_PATH
    val kafkaBootstrap = BOOTSTRAP_SERVER
    val topicName = REVIEW_TOPIC
    val batchSize = 1000

    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "4")

    // === Lecture JSON ligne-par-ligne ===
    val df = spark.read
      .format("json")
      .load(inputPath)

    val windowSpec = Window.orderBy(col("date").asc)

    val indexedDF = df.select(
      col("review_id"),
      col("user_id"),
      col("business_id"),
      col("stars"),
      col("useful"),
      col("funny"),
      col("text"),
      col("date"),
      row_number().over(windowSpec).alias("id_date")
    )

    val dfWithBatch = indexedDF.withColumn("batch_id", (col("id_date") - 1) / batchSize)
    val totalBatches = dfWithBatch.select("batch_id").distinct().count().toInt

    println("\n=============================================================================")
    println(s"Nombre total de batchs à envoyer à Kafka : $totalBatches")
    println("=============================================================================\n")

    for (i <- 0 until totalBatches) {
        val start = i * batchSize + 1
        val end = start + batchSize - 1

        val batchDF = dfWithBatch
        .filter(col("id_date").between(start, end))

        val kafkaDF = batchDF.selectExpr(
          "CAST(review_id AS STRING) AS key",
          "to_json(struct(*)) AS value"
        )

        kafkaDF.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("topic", topicName)
            .option("checkpointLocation", s"/tmp/kafka-checkpoint-batch-$i")
            .save()

        println(s"✔ Batch $i envoyé dans le topic Kafka '$topicName'")
    }

    spark.stop()
    println("✅ Tous les batchs ont été envoyés à Kafka.")
  }
}
