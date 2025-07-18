package com.example
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.annotation.tailrec
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}
import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, TOP_CATEGORIES_TABLE, USER_TABLE}
import org.apache.spark.sql.functions._

import DataSourceReader.loadOrCreateArtefactSafe
import Config.{
  BOOTSTRAP_SERVER,BUSINESS_ARTEFACT_PATH,BUSINESS_JSON_PATH,BUSINESS_SCHEMA,
  REVIEW_SCHEMA, REVIEW_TOPIC, USER_ARTEFACT_PATH, USER_JSON_PATH, USER_SCHEMA, REVIEW_JSON_PATH, REVIEW_ARTEFACT_PATH
}
import UpdateDatabase.{
  updateReviewTable,
  processBusinessState,
  processUsersStates
}

object Consumer {

// ================== AGGREGATIONS BUSINESS ==================

def processBusinessOverview(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Vue d'ensemble des entreprises avec métriques principales"""
  
  val reviewStats = reviewsDF
    .groupBy("business_id")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("average_rating"),
      collect_list("stars").alias("rating_list"),
      max("date").alias("last_review_date")
    )
  
  // Calcul de la tendance récente (3 derniers mois)
  val recentReviews = reviewsDF
    .filter(col("date") >= date_sub(current_date(), 90))
    .groupBy("business_id")
    .agg(
      avg("stars").alias("recent_average"),
      count("*").alias("recent_reviews_count")
    )
  
  val businessOverview = businessDF
    .join(reviewStats, Seq("business_id"), "left")
    .join(recentReviews, Seq("business_id"), "left")
    .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
    .withColumn("average_rating", round(coalesce(col("average_rating"), lit(0)), 2))
    .withColumn("recent_average", round(coalesce(col("recent_average"), col("average_rating")), 2))
    .select(
      "business_id", "name", "address", "city", "state", "categories", "is_open",
      "total_reviews", "average_rating", "recent_average", "last_review_date"
    )
  
  // Sauvegarde en base
  businessOverview.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "business_overview"))
    .mode("overwrite")
    .save()
    
  businessOverview
}

def processRatingDistribution(spark: SparkSession, reviewsDF: DataFrame): DataFrame = {
  """Distribution des notes par entreprise"""
  
  val ratingDistribution = reviewsDF
    .groupBy("business_id", "stars")
    .agg(count("*").alias("count"))
    .withColumn("rating", col("stars").cast("int"))
    .select("business_id", "rating", "count")
  
  // Sauvegarde en base
  ratingDistribution.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "rating_distribution"))
    .mode("overwrite")
    .save()
    
  ratingDistribution
}

// ================== AGGREGATIONS TEMPORELLES ==================

def processTemporalAnalysis(spark: SparkSession, reviewsDF: DataFrame, period: String = "month"): DataFrame = {
  """Analyse temporelle des avis par période"""
  
  val periodCol = period match {
    case "month" => date_format(col("date"), "yyyy-MM")
    case "quarter" => concat(year(col("date")), lit("-Q"), quarter(col("date")))
    case "year" => year(col("date")).cast("string")
    case _ => date_format(col("date"), "yyyy-MM")
  }
  
  val temporalStats = reviewsDF
    .withColumn("period", periodCol)
    .groupBy("business_id", "period")
    .agg(
      avg("stars").alias("avg_rating"),
      count("*").alias("review_count"),
      sum("useful").alias("useful_total"),
      sum("funny").alias("funny_total"),
      sum("cool").alias("cool_total")
    )
    .withColumn("avg_rating", round(col("avg_rating"), 2))
    .withColumn("period_type", lit(period))
    .orderBy("business_id", "period")
  
  // Sauvegarde en base
  temporalStats.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "temporal_analysis"))
    .mode("overwrite")
    .save()
    
  temporalStats
}

// def processTrendAnalysis(spark: SparkSession, reviewsDF: DataFrame): DataFrame = {
//   """Détection de tendances pour chaque entreprise"""
  
//   import spark.implicits._
  
//   // Calcul des moyennes par trimestre
//   val quarterlyStats = reviewsDF
//     .withColumn("quarter", concat(year(col("date")), lit("-Q"), quarter(col("date"))))
//     .groupBy("business_id", "quarter")
//     .agg(avg("stars").alias("avg_rating"))
//     .withColumn("quarter_num", 
//       when(col("quarter").endsWith("Q1"), 1)
//       .when(col("quarter").endsWith("Q2"), 2)
//       .when(col("quarter").endsWith("Q3"), 3)
//       .otherwise(4)
//     )
  
//   // Calcul de tendance (derniers 3 trimestres vs premiers 3 trimestres)
//   val trendAnalysis = quarterlyStats
//     .withColumn("rn", row_number().over(
//       Window.partitionBy("business_id").orderBy(col("quarter"))
//     ))
//     .groupBy("business_id")
//     .agg(
//       avg(when(col("rn") <= 3, col("avg_rating"))).alias("early_avg"),
//       avg(when(col("rn") > col("rn").max() - 3, col("avg_rating"))).alias("recent_avg"),
//       count("*").alias("quarters_count")
//     )
//     .withColumn("trend", 
//       when(col("recent_avg") - col("early_avg") > 0.2, "amélioration")
//       .when(col("recent_avg") - col("early_avg") < -0.2, "dégradation")
//       .otherwise("stable")
//     )
//     .filter(col("quarters_count") >= 3)
  
//   // Sauvegarde en base
//   trendAnalysis.write
//     .format("jdbc")
//     .options(DB_CONFIG + ("dbtable" -> "trend_analysis"))
//     .mode("overwrite")
//     .save()
    
//   trendAnalysis
// }

// ================== AGGREGATIONS UTILISATEURS ==================

def processUserAnalysis(spark: SparkSession, userDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Analyse des utilisateurs par entreprise"""
  
  val userBusinessStats = reviewsDF
    .groupBy("business_id", "user_id")
    .agg(
      avg("stars").alias("avg_rating_business"),
      count("*").alias("reviews_count_business"),
      sum("useful").alias("useful_votes_business"),
      max("date").alias("last_review_date")
    )
    .join(userDF.select("user_id", "name", "review_count", "useful", "elite"), "user_id")
    .withColumn("avg_rating_business", round(col("avg_rating_business"), 2))
  
  // Statistiques agrégées par entreprise
  val businessUserStats = userBusinessStats
    .groupBy("business_id")
    .agg(
      countDistinct("user_id").alias("unique_users"),
      avg("reviews_count_business").alias("avg_reviews_per_user"),
      sum(when(col("elite").isNotNull, 1).otherwise(0)).alias("elite_users_count"),
      max("useful_votes_business").alias("max_useful_votes")
    )
    .withColumn("avg_reviews_per_user", round(col("avg_reviews_per_user"), 1))
  
  // Sauvegarde des stats par entreprise
  businessUserStats.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "business_user_stats"))
    .mode("overwrite")
    .save()
  
  businessUserStats
}

def processTopReviewers(spark: SparkSession, userDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Top contributeurs par entreprise"""
  
  val topReviewers = reviewsDF
    .groupBy("business_id", "user_id")
    .agg(
      avg("stars").alias("avg_rating"),
      count("*").alias("review_count"),
      sum("useful").alias("useful_votes")
    )
    .join(userDF.select("user_id", "name", "elite"), "user_id")
    .withColumn("avg_rating", round(col("avg_rating"), 2))
    .withColumn("rn", row_number().over(
      Window.partitionBy("business_id").orderBy(desc("useful_votes"))
    ))
    .filter(col("rn") <= 10) // Top 10 par entreprise
    .select("business_id", "user_id", "name", "avg_rating", "review_count", "useful_votes", "elite", "rn")
  
  // Sauvegarde en base
  topReviewers.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "top_reviewers"))
    .mode("overwrite")
    .save()
    
  topReviewers
}

// ================== FONCTION PRINCIPALE ==================

def processAllAnalytics(spark: SparkSession, businessDF: DataFrame, userDF: DataFrame, reviewsDF: DataFrame): Unit = {
  """Traite toutes les analyses et les sauvegarde en base"""
  
  println("🔄 Traitement des analyses...")
  
  // 1. Vue d'ensemble des entreprises
  val businessOverview = processBusinessOverview(spark, businessDF, reviewsDF)
  println(s"✅ Business overview: ${businessOverview.count()} entreprises")
  
  // 2. Distribution des notes
  val ratingDist = processRatingDistribution(spark, reviewsDF)
  println(s"✅ Rating distribution: ${ratingDist.count()} entrées")
  
  // 3. Analyse temporelle (mensuelle)
  val temporalAnalysis = processTemporalAnalysis(spark, reviewsDF, "month")
  println(s"✅ Temporal analysis: ${temporalAnalysis.count()} périodes")
  
  // 4. Analyse des tendances
  // val trendAnalysis = processTrendAnalysis(spark, reviewsDF)
  // println(s"✅ Trend analysis: ${trendAnalysis.count()} entreprises")
  
  // 5. Analyse des utilisateurs
  val userAnalysis = processUserAnalysis(spark, userDF, reviewsDF)
  println(s"✅ User analysis: ${userAnalysis.count()} entreprises")
  
  // 6. Top reviewers
  val topReviewers = processTopReviewers(spark, userDF, reviewsDF)
  println(s"✅ Top reviewers: ${topReviewers.count()} contributeurs")
  
  println("🎉 Toutes les analyses terminées et sauvegardées!")
}

// ================== INTEGRATION DANS VOTRE PIPELINE ==================

def integrateInYourPipeline(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame, allReviews: DataFrame): Unit = {
  """Intégration dans votre pipeline existant"""
  
  // Filtrer les reviews pour ne traiter que les nouvelles données
  val activeBusinessIds = allReviews.select("business_id").distinct()
  val activeUserIds = allReviews.select("user_id").distinct()
  
  val filteredBusiness = businessDF.join(activeBusinessIds, "business_id")
  val filteredUsers = usersDF.join(activeUserIds, "user_id")
  
  // Traitement des analyses
  processAllAnalytics(spark, filteredBusiness, filteredUsers, allReviews)
}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "100")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val usersDF = loadOrCreateArtefactSafe(
        spark,
        USER_JSON_PATH,
        USER_ARTEFACT_PATH,
        USER_SCHEMA,
        Seq("user_id", "name", "fans", "elite", "friends", "yelping_since"),
      )
      val userDF = usersDF.withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))

      val businessDF = loadOrCreateArtefactSafe(
        spark,
        BUSINESS_JSON_PATH,
        BUSINESS_ARTEFACT_PATH,
        BUSINESS_SCHEMA,
        Seq("business_id", "name", "city", "address" ,"latitude", "longitude","state", "categories", "is_Open"),
      )

      consumeKafkaTopic(spark, businessDF, userDF)
    } catch {
      case e: java.io.FileNotFoundException =>
        println(s"Fichier introuvable : ${e.getMessage}")
        e.printStackTrace()

      case e: org.apache.spark.sql.AnalysisException =>
        println(s"Erreur d'analyse Spark SQL : ${e.getMessage}")
        e.printStackTrace()

      case e: java.text.ParseException =>
        println(s"Erreur de parsing (date ou autre) : ${e.getMessage}")
        e.printStackTrace()

      case e: IllegalArgumentException =>
        println(s"Argument invalide : ${e.getMessage}")
        e.printStackTrace()

      case e: Exception =>
        println(s"Erreur inattendue : ${e.getMessage}")
        e.printStackTrace()
    }
    spark.streams.awaitAnyTermination()
  }

  private def consumeKafkaTopic(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame): Unit = {

    val maybeKafkaDF = tryConnect(spark, attempt=1, retries=5, delaySeconds=5)

    maybeKafkaDF match {
      case Some(kafkaStreamDF) =>
          val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          
          val parsedMessages = messages.select(
            from_json(col("value"), REVIEW_SCHEMA).as("data")
          ).select("data.*")
          
          parsedMessages.writeStream
            .foreachBatch { (newBatch: DataFrame, batchId: Long) =>

              val allReviews = updateReviewTable(spark, newBatch)
              integrateInYourPipeline(spark, businessDF, usersDF, allReviews)

              println(s"Batch $batchId traité et statistiques insérées.")
            }
            .outputMode("append")
            .start()
            .awaitTermination()

      case None =>
        println("Le topic Kafka n’est pas disponible. Fermeture de l'application.")
        System.exit(1)
    }
  }

  @tailrec
  private def tryConnect(spark: SparkSession, attempt: Int, retries: Int, delaySeconds: Int): Option[DataFrame] = {
      println(s"Tentative $attempt/$retries de connexion au topic Kafka...")
      Try {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
          .option("subscribe", REVIEW_TOPIC)
          .option("startingOffsets", "earliest")
          .load()
      } match {
        // Connection reussi
        case Success(df) =>
          println(s"Connexion établie avec le topic Kafka '$REVIEW_TOPIC'")
          Some(df)

        // Échec Connection Nouvelle tentative
        case Failure(e) if attempt < retries =>
          println(s"Échec tentative $attempt : ${e.getMessage}")
          Thread.sleep(delaySeconds * 1000)
          tryConnect(spark, attempt + 1, retries, delaySeconds)
        
        // Échec Connection arret de l'application
        case Failure(e) =>
          println(s"Échec après $retries tentatives. Abandon.")
          None
      }
  }
}
