import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataSourceReader {

  def loadOrCreateArtefactSafe(
      spark: SparkSession,
      jsonPath: String,
      artefactPath: String,
      schema: StructType,
      columnsToKeep: Seq[String],
      format: String = "orc"
  ): DataFrame = {
    try {
      println(s"Tentative de chargement de l’artefact : $artefactPath...")
      val df = format.toLowerCase match {
        case "orc" => spark.read.orc(artefactPath)
        case "parquet" => spark.read.parquet(artefactPath)
        case _ => throw new IllegalArgumentException(s"Format non supporté : $format")
      }
      println("Artefact trouvé et chargé.")
      df
    } catch {
      case _: Exception =>
        println(s"Aucun artefact trouvé à $artefactPath. Création depuis le JSON...")
        val df = spark.read.schema(schema).json(jsonPath)
          .select(columnsToKeep.head, columnsToKeep.tail: _*)
  
        format.toLowerCase match {
          case "orc" => df.write.mode("overwrite").orc(artefactPath)
          case "parquet" => df.write.mode("overwrite").parquet(artefactPath)
          case _ => throw new IllegalArgumentException(s"Format non supporté : $format")
        }
        df
    }
  }
}