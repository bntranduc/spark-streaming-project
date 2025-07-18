package com.example

import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, TOP_CATEGORIES_TABLE, USER_TABLE}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UpdateDatabase {
    def updateReviewTable(spark: SparkSession, batchDF: DataFrame): DataFrame = {
        val df_review_db = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .load()

        val new_reviews = batchDF
          .join(df_review_db.select("review_id"), Seq("review_id"), "left_anti")
          .distinct()

        new_reviews.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .mode("append")
          .save()

        df_review_db.union(new_reviews)
  }

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
      userStats.write
        .format("jdbc")
        .options(DB_CONFIG + ("dbtable" -> USER_TABLE))
        .mode("overwrite")
        .save()
    userStats
  }

  def processBusinessState(spark: SparkSession, allBusiness: DataFrame, reviews: DataFrame): DataFrame = {
    val reviewStats = reviews
      .groupBy("business_id")
      .agg(
        count("*").alias("total_reviews"),
        sum("useful").alias("useful_count"),
        avg("useful").alias("avg_useful"),
        sum("funny").alias("funny_count"),
        avg("funny").alias("avg_funny"),
        avg("stars").alias("avg_stars")
      )
      .withColumn("rounded_rating", round(col("avg_stars")).cast("int"))
      .orderBy(desc("total_reviews"))

    val businessStates = allBusiness
      .join(reviewStats, Seq("business_id"), "inner")

    businessStates.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
      .mode("overwrite")
      .save()
    businessStates
  }
}