import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.io.Source

import Config._

object FileSplitterOptimized {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("File Splitter Optimized")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    splitFileInBatches(spark, REVIEW_JSON_PATH, REVIEW_SCHEMA, REVIEW_OUTPUT_PATH, BATCH_SIZE, REVIEW_CHECKPOINT_FILE)

    spark.stop()
  }

  def splitFileInBatches(spark: SparkSession, jsonPath: String, schema: StructType, outputPath: String, batchSize: Int, checkpointFile: String): Unit = {
    import spark.implicits._

    var lastRowIndex = (readLastBatchId(checkpointFile) + 1) * batchSize
    var batchId = lastRowIndex / batchSize

    val df = spark.read.schema(schema).json(jsonPath)

    val dfWithId = df.withColumn("row_id", monotonically_increasing_id())

    while (true) {
      val batchDF = dfWithId.filter($"row_id" >= lastRowIndex && $"row_id" < lastRowIndex + batchSize)
        .drop("row_id")

      val count = batchDF.count()

      if (count == 0) {
        println("No more data to process.")
        return
      }

      batchDF.write.mode(SaveMode.Overwrite).json(s"$outputPath/batch_$batchId")

      println(s"Batch $batchId : $count rows written")
      writeLastBatchId(checkpointFile, batchId)

      batchId += 1
      lastRowIndex += batchSize

      Thread.sleep(2000)
    }
  }

  def readLastBatchId(path: String): Int = {
    if (!Files.exists(Paths.get(path))) {
      return -1
    }
    val source = Source.fromFile(path)
    val nombre = source.mkString.trim.toInt
    source.close()
    nombre
  }

  def writeLastBatchId(path: String, batchId: Int): Unit = {
    Files.write(Paths.get(path), batchId.toString.getBytes(StandardCharsets.UTF_8))
  }
}
