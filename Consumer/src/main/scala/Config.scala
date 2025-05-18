import org.apache.spark.sql.types._

object Config {
  //Dataset path
  val DATASET_PATH = sys.env.getOrElse("DATASET_PATH", "../yelp_dataset/")
  
  val BUSINESS_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_business.json"
  val REVIEW_JSON_PATH: String = DATASET_PATH + "yelp_academic_dataset_review.json"
  val USER_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_user.json"

  // Kafka
  //val BOOTSTRAP_SERVER: String = "localhost:9092"
  val BOOTSTRAP_SERVER: String = "kafka:9092"

  // Base de données
  val DB_URL: String = "jdbc:postgresql://localhost:5432/mydatabase"
  val DB_USER: String = "user"
  val DB_PASSWORD: String = "password"
  val DB_DRIVER: String = "org.postgresql.Driver"

  // Business
  val BUSINESS_TOPIC: String = "yelp-topic-business"
  val BUSINESS_ARTEFACT_PATH = DATASET_PATH + "/business.parquet"
  val BUSINESS_SCHEMA: StructType = StructType(List(
    StructField("business_id", StringType, true),
    StructField("name", StringType, true),
    StructField("address", StringType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("postal_code", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true),
    StructField("stars", DoubleType, true),
    StructField("review_count", IntegerType, true),
    StructField("is_open", IntegerType, true),
    StructField("attributes", StringType, true),
    StructField("categories", StringType, true),
    StructField("hours", StringType, true)
  ))
  val BUSINESS_TABLE: String = "business_table"

  // Review
  val REVIEW_TOPIC: String = "yelp-topic-review"
  val REVIEW_SCHEMA: StructType = StructType(List(
    StructField("review_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("business_id", StringType, true),
    StructField("stars", DoubleType, true),
    StructField("useful", IntegerType, true),
    StructField("funny", IntegerType, true),
    StructField("cool", IntegerType, true),
    StructField("text", StringType, true),
    StructField("date", StringType, true)
  ))
  val REVIEW_TABLE: String = "review_table"

  // User
  val USER_TOPIC: String = "yelp-topic-user"
  val USER_ARTEFACT_PATH = DATASET_PATH + "/users.parquet"
  val USER_SCHEMA: StructType = StructType(List(
    StructField("user_id", StringType, true),
    StructField("name", StringType, true),
    StructField("review_count", IntegerType, true),
    StructField("yelping_since", StringType, true),
    StructField("useful", IntegerType, true),
    StructField("funny", IntegerType, true),
    StructField("cool", IntegerType, true),
    StructField("elite", StringType, true),
    StructField("friends", StringType, true),
    StructField("fans", IntegerType, true),
    StructField("average_stars", DoubleType, true),
    StructField("compliment_hot", IntegerType, true),
    StructField("compliment_more", IntegerType, true),
    StructField("compliment_profile", IntegerType, true),
    StructField("compliment_cute", IntegerType, true),
    StructField("compliment_list", IntegerType, true),
    StructField("compliment_note", IntegerType, true),
    StructField("compliment_plain", IntegerType, true),
    StructField("compliment_cool", IntegerType, true),
    StructField("compliment_funny", IntegerType, true),
    StructField("compliment_writer", IntegerType, true),
    StructField("compliment_photos", IntegerType, true)
  ))
  val USER_TABLE: String = "user_table"
}
