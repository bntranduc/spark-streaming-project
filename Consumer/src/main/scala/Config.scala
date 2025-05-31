import org.apache.spark.sql.types._

object Config {
  //Dataset path
  val DATASET_PATH = sys.env.getOrElse("DATASET_PATH", "../yelp_dataset")
  val BUSINESS_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_business.json"
  val USER_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_user.json"

  // Kafka
  val BOOTSTRAP_SERVER = sys.env.getOrElse("KAFKA_HOST", "localhost:9092")

  // Base de données
  val DB_USER: String = sys.env.getOrElse("DATABASE_USER", "divinandretomadam")
  val DB_PASSWORD: String = sys.env.getOrElse("DATABASE_PASSWORD", "oDAnmvidrTnmeiAa")
  val DB_NAME: String = sys.env.getOrElse("DATABASE_NAME", "spark_streaming_db")
  val DB_HOST: String = sys.env.getOrElse("DATABASE_HOST", "localhost")
  val DB_PORT: String = sys.env.getOrElse("DATABASE_PORT", "5432")
  val DB_URL: String = s"jdbc:postgresql://$DB_HOST:$DB_PORT/$DB_NAME"
  val DB_DRIVER: String = "org.postgresql.Driver"

  // Business
  val BUSINESS_ARTEFACT_PATH = DATASET_PATH + "/business.parquet"
  val BUSINESS_SCHEMA: StructType = StructType(List(
    StructField("business_id", StringType, true),
    StructField("name", StringType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("categories", StringType, true),
  ))
  val BUSINESS_TABLE: String = "business_table"

  // Review
  val REVIEW_TOPIC: String = "yelp-topic-review-1"
  val REVIEW_SCHEMA: StructType = StructType(List(
    StructField("review_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("business_id", StringType, true),
    StructField("stars", DoubleType, true),
    StructField("useful", IntegerType, true),
    StructField("funny", IntegerType, true),
    StructField("cool", IntegerType, true),
    StructField("text", StringType, true),
    StructField("date", StringType, true),
    StructField("id_date", IntegerType, true)
  ))
  val REVIEW_TABLE: String = "review_table"

  // User
  val USER_ARTEFACT_PATH = DATASET_PATH + "/users.parquet"
  val USER_SCHEMA: StructType = StructType(List(
    StructField("user_id", StringType, true),
    StructField("name", StringType, true)
  ))
  val USER_TABLE: String = "user_table"

  // top Business
  val TOP_FUN_BUSINESS_TABLE: String = "top_fun_business_table"
  val TOP_USEFULL_USER_TABLE: String = "top_usefull_user_table"
  val TOP_FAITHFUL_USER_TABLE: String = "top_faithful_user_table"
}
