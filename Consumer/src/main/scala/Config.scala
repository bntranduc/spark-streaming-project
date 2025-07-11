package com.example

import org.apache.spark.sql.types.{StructField, _}

object Config {
  //Dataset path
  private val DATASET_PATH = sys.env.getOrElse("DATASET_PATH", "../yelp_dataset")
  val BUSINESS_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_business.json"
  val USER_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_user.json"
  val REVIEW_JSON_PATH: String = DATASET_PATH + "/yelp_academic_dataset_review.json"

  // Kafka
  val BOOTSTRAP_SERVER: String = sys.env.getOrElse("KAFKA_HOST", "localhost:9092")

  // Base de données
  private val DB_NAME: String = sys.env.getOrElse("DATABASE_NAME", "spark_streaming_db")
  private val DB_HOST: String = sys.env.getOrElse("DATABASE_HOST", "localhost")
  private val DB_PORT: String = sys.env.getOrElse("DATABASE_PORT", "5432")

  val DB_CONFIG: Map[String, String] = Map(
    "url" -> s"jdbc:postgresql://$DB_HOST:$DB_PORT/$DB_NAME",
    "user" -> sys.env.getOrElse("DATABASE_USER", "divinandretomadam"),
    "password" -> sys.env.getOrElse("DATABASE_PASSWORD", "oDAnmvidrTnmeiAa"),
    "driver" -> "org.postgresql.Driver"
  )

  // Business
  val BUSINESS_ARTEFACT_PATH: String = DATASET_PATH + "/business.parquet"
  val BUSINESS_SCHEMA: StructType = StructType(
    List(
      StructField("name", StringType, true),
      StructField("city", StringType, true),
      StructField("business_id", StringType, true),
      StructField("latitude", DoubleType, true),
      StructField("address", StringType, true),
      StructField("longitude", DoubleType, true),
      StructField("state", StringType, true),
      StructField("is_open", IntegerType, true),
      StructField("categories", StringType, true),
    ))

  // Review
  val REVIEW_ARTEFACT_PATH: String = DATASET_PATH + "/reviews.parquet"
  val REVIEW_TOPIC: String = "yelp-topic-review"
  val REVIEW_SCHEMA: StructType = StructType(
    List(
      StructField("review_id", StringType, true),
      StructField("business_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("stars", DoubleType, true),
      StructField("useful", IntegerType, true),
      StructField("funny", IntegerType, true),
      StructField("cool", IntegerType, true),
      StructField("text", StringType, true),
      StructField("date", StringType, true),
      StructField("id_date", IntegerType, true)
  ))

  // User
  val USER_ARTEFACT_PATH: String = DATASET_PATH + "/users.parquet"
  val USER_SCHEMA: StructType = StructType(List(
    StructField("user_id", StringType, true),
    StructField("friends", StringType, true),
    StructField("elite", StringType, true),
    StructField("fans", IntegerType, true),
    StructField("yelping_since", TimestampType, true),
    StructField("name", StringType, true)
  ))

  // REVIEWS
  val REVIEW_TABLE: String = "review_table"
  val REVIEW_DISTRIBUTION_TABLE: String = "review_distribution_table"
  val SEASONAL_REVIEW_STARS_TABLE= "seasonal_review_stats"
  val WEAKLY_REVIEW_STARS = "weekly_review_stats"
  val REVIEW_DISTRIBUTION_BY_USEFUL_TABLE = "review_distribution_useful"

  // BUSINESS
  val BUSINESS_TABLE: String = "business_table"
  val TOP_CATEGORIES_TABLE: String = "top_categories_table"

  // USER
  val USER_TABLE: String = "user_table"
}
