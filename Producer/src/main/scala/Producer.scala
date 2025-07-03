package com.example

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.io._
import com.example.Config._

object Producer {

  def main(args: Array[String]): Unit = {

    // === Paramètres ===
    val inputPath = REVIEW_JSON_PATH
    val kafkaBootstrap = BOOTSTRAP_SERVER
    val topicName = REVIEW_TOPIC
    val batchSize = BATCH_SIZE
    var lastBatchSent = -1

    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.sparkContext.setLogLevel("ERROR")

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

    val stateFile = new File(SAVE_BATCH_STATE_FILE)
    if (stateFile.exists()) {
      val source = scala.io.Source.fromFile(stateFile)
      lastBatchSent = source.getLines().next().toInt
      source.close()
    }

    println("\n=============================================================================")
    println(s"Nombre total de batchs déjà envoyés, : ${lastBatchSent + 1}")
    println(s"Nombre total de batchs à envoyer à Kafka, : ${totalBatches - lastBatchSent}")
    println("=============================================================================\n")

    for (i <- (lastBatchSent + 1) until totalBatches) {
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
            // Kafka producer timeout configurations
            .option("kafka.request.timeout.ms", "300000")        // 5 minutes
            .option("kafka.delivery.timeout.ms", "360000")       // 6 minutes  
            .option("kafka.max.block.ms", "300000")             // 5 minutes
            .option("kafka.retries", "3")                        // Retry failed sends
            .option("kafka.retry.backoff.ms", "1000")           // Wait between retries
            // Batch size optimizations
            .option("kafka.batch.size", "32768")                 // 32KB batches
            .option("kafka.linger.ms", "100")                    // Wait 100ms to batch
            .option("kafka.buffer.memory", "67108864")           // 64MB buffer
            // Compression for better throughput
            .option("kafka.compression.type", "snappy")
            .save()
            
        val writer = new PrintWriter(new File(SAVE_BATCH_STATE_FILE))
        writer.write(i.toString)
        writer.close()
        
        println(s"✔ Batch $i envoyé dans le topic Kafka '$topicName'")
    }

    spark.stop()
    println("✅ Tous les batchs ont été envoyés à Kafka.")
  }
}
