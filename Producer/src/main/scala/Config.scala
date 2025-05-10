import org.apache.spark.sql.types._

object Config {

  val BATCH_SIZE = 1000

  // Kafka
  val BOOTSTRAP_SERVER: String = "localhost:9092"

  // Base de données
  val DB_URL: String = "jdbc:postgresql://localhost:5432/mydatabase"
  val DB_USER: String = "user"
  val DB_PASSWORD: String = "password"
  val DB_DRIVER: String = "org.postgresql.Driver"


  val DATASET_PATH = sys.env.getOrElse("DATASET_PATH", "../yelp_dataset/")
  
  // Business
  val BUSINESS_TOPIC: String = "yelp-topic-business"
  val BUSINESS_JSON_PATH: String = DATASET_PATH + "yelp_academic_dataset_business.json"
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
  val BUSINESS_OUTPUT_PATH: String = "output_batches/business/"
  val BUSINESS_CHECKPOINT_FILE: String = "output_batches/checkpoint_business.txt"

  // Review
  val REVIEW_TOPIC: String = "yelp-topic-review"
  val REVIEW_JSON_PATH: String = DATASET_PATH + "yelp_academic_dataset_review.json"
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
  val REVIEW_OUTPUT_PATH: String = "output_batches/review/"
  val REVIEW_CHECKPOINT_FILE: String = "output_batches/checkpoint_review.txt"

  // User
  val USER_TOPIC: String = "yelp-topic-user"
  val USER_JSON_PATH: String = DATASET_PATH + "yelp_academic_dataset_user.json"
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
  val USER_OUTPUT_PATH: String = "output_batches/user/"
  val USER_CHECKPOINT_FILE: String = "output_batches/checkpoint_user.txt"

  // Tip
  val TIP_TOPIC: String = "yelp-topic-tip"
  val TIP_JSON_PATH: String = DATASET_PATH + "yelp_academic_dataset_tip.json"
  val TIP_SCHEMA: StructType = StructType(List(
    StructField("user_id", StringType, true),
    StructField("business_id", StringType, true),
    StructField("text", StringType, true),
    StructField("date", StringType, true),
    StructField("compliment_count", IntegerType, true)
  ))
  val TIP_TABLE: String = "tip_table"
  val TIP_OUTPUT_PATH: String = "output_batches/tip/"
  val TIP_CHECKPOINT_FILE: String = "output_batches/checkpoint_tip.txt"
}
