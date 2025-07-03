package com.example

import Config.{DB_CONFIG, REVIEW_TABLE, USER_TABLE}
import UpdateDatabase.updateUserTable
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, current_date, date_format, date_sub, datediff, desc, expr, first, lag, round, stddev, sum, to_date, unix_timestamp, when}

object UserStatsProcessor {

  def processUsersStates(allUsersDF: DataFrame, df_reviews_db: DataFrame): DataFrame = {
    val filteredUsers = allUsersDF
      .join(df_reviews_db, Seq("user_id"), "inner")

    val userStats = filteredUsers
      .groupBy("user_id")
      .agg(
        count("*").alias("total_reviews"),
        sum("useful").alias("useful_count"),
        avg("useful").alias("avg_useful"),
        sum("funny").alias("funny_count"),
        avg("funny").alias("avg_funny"),
        avg("stars").alias("avg_stars"),
        stddev("stars").alias("rating_stddev"),
        sum(when(col("stars") === 1, 1).otherwise(0)).alias("count_1star"),
        sum(when(col("stars") === 5, 1).otherwise(0)).alias("count_5star"),
        sum(
          when(col("stars") <= 2, 1).otherwise(0)
        ).alias("low_ratings_count"),
        expr("sum(CASE WHEN stars <= 2 THEN 1 ELSE 0 END) * 1.0 / count(*)").alias("low_rating_ratio")
      )
      .orderBy(desc("total_reviews"))

    updateUserTable(userStats)
    userStats
  }

  def processGeneralUsersStats(userStats: DataFrame): Unit = {
    // 1. Distribution des utilisateurs par nombre de reviews
    val reviewsDistribution = userStats
      .withColumn("review_range",
          when(col("total_reviews") <= 5, "1-5 reviews")
          .when(col("total_reviews") <= 10, "6-10 reviews")
          .when(col("total_reviews") <= 50, "11-50 reviews")
          .when(col("total_reviews") <= 100, "51-100 reviews")
          .otherwise("100+ reviews"))
      .groupBy("review_range")
      .agg(
        count("*").alias("nb_users"),
        avg("avg_stars").alias("avg_rating_given"),
        avg("low_rating_ratio").alias("avg_low_rating_ratio")
      )
      .orderBy(
          when(col("review_range") === "1-5 reviews", 2)
          .when(col("review_range") === "6-10 reviews", 3)
          .when(col("review_range") === "11-50 reviews", 4)
          .when(col("review_range") === "51-100 reviews", 5)
          .otherwise(6)
      )

    reviewsDistribution.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "users_by_review_count_distribution"))
      .mode("overwrite")
      .save()

  }

  def processUsersSeverityStats(userStats: DataFrame): Unit = {
    // Catégoriser les utilisateurs par sévérité de notation
    val severityDistribution = userStats
      .withColumn("severity_category",
        when(col("avg_stars") <= 1.5, "Très sévère (≤1.5★)")
          .when(col("avg_stars") <= 2.5, "Sévère (1.5-2.5★)")
          .when(col("avg_stars") <= 3.5, "Modéré (2.5-3.5★)")
          .when(col("avg_stars") <= 4.5, "Clément (3.5-4.5★)")
          .otherwise("Très clément (>4.5★)"))
      .groupBy("severity_category")
      .agg(
        count("*").alias("nb_users"),
        avg("total_reviews").alias("avg_reviews_per_user"),
        avg("low_rating_ratio").alias("avg_low_rating_ratio"),
        sum("total_reviews").alias("total_reviews")
      )
      .orderBy("severity_category")

    severityDistribution.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "users_by_severity_distribution"))
      .mode("overwrite")
      .save()

    // Statistiques sur les utilisateurs "sévères" (moyenne <= 2.5)
    val severeUsersStats = userStats
      .filter(col("avg_stars") <= 2.5)
      .agg(
        count("*").alias("nb_severe_users"),
        avg("total_reviews").alias("avg_reviews_per_severe_user"),
        avg("low_rating_ratio").alias("avg_low_rating_ratio_severe"),
        sum("total_reviews").alias("total_reviews_by_severe_users")
      )

    severeUsersStats.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "severe_users_stats"))
      .mode("overwrite")
      .save()
  }

  def detectPolarizedUsers(df_reviews: DataFrame): Unit = {
    val polarizedUsers = df_reviews
      .withColumn("polarization_score",
        (col("count_1star") + col("count_5star")) / col("total_reviews"))
      .filter(col("polarization_score") >= 0.8)  // Au moins 80% de notes extrêmes
      .filter(col("total_reviews") >= 5)  // Minimum 5 reviews pour éviter le bruit

    polarizedUsers.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "polarized_users"))
      .mode("overwrite")
      .save()
  }

  def detectInfluentialUsers(users: DataFrame): Unit = {
    val influentialUsers = users
      .filter(col("avg_useful") >= 3.0)  // Moyenne de 3 votes utiles par review
      .orderBy(desc("useful_count"))

    influentialUsers.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "influential_users"))
      .mode("overwrite")
      .save()
  }

  def detectSerialOffenders(df_reviews: DataFrame): Unit = {
    val offenders = df_reviews
      .filter(col("stars") <= 2)
      .groupBy("user_id", "business_id")
      .count()
      .filter(col("count") >= 3)  // 3+ mauvaises notes sur le même business
      .groupBy("user_id")
      .agg(count("*").alias("targeted_businesses"))
      .filter(col("targeted_businesses") >= 2)  // Cible au moins 2 établissements

    offenders.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "serial_offenders"))
      .mode("overwrite")
      .save()
  }
}
