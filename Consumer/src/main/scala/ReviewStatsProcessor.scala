package com.example

import Config.{DB_CONFIG, REVIEW_DISTRIBUTION_BY_USEFUL_TABLE, REVIEW_DISTRIBUTION_TABLE, REVIEW_TABLE, SEASONAL_REVIEW_STARS_TABLE, WEAKLY_REVIEW_STARS}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ReviewStatsProcessor {
  def processReviewDistribution(dfReviews: DataFrame): Unit = {
    val noteDistribution = dfReviews
      .groupBy("stars")
      .count()
      .withColumnRenamed("count", "nb_notes")
      .orderBy("stars")

    noteDistribution.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> REVIEW_DISTRIBUTION_TABLE))
      .mode("overwrite")
      .save()
  }

  def processMonthlyReviewStats(dfReviews: DataFrame): Unit = {
    val seasonalStats = dfReviews
      .withColumn("month_name", date_format(col("date"), "MMMM"))
      .withColumn("month_num", month(col("date")))
      .groupBy("month_num", "month_name")
      .agg(
        count("*").alias("total_reviews"),
        avg("stars").alias("avg_stars")
      )
      .orderBy("month_num")

    seasonalStats.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> SEASONAL_REVIEW_STARS_TABLE))
      .mode("overwrite")
      .save()
  }

  def processWeeklyReviewStats(dfReviews: DataFrame): Unit = {
    val weeklyStats = dfReviews
      .withColumn("day_name", date_format(col("date"), "EEEE"))
      .withColumn("day_num", expr("EXTRACT(DAYOFWEEK FROM date)"))
      .groupBy("day_num", "day_name")
      .agg(
        count("*").alias("total_reviews"),
        avg("stars").alias("avg_stars")
      )
      .orderBy("day_num")

    weeklyStats.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> WEAKLY_REVIEW_STARS))
      .mode("overwrite")
      .save()
  }

  def processReviewDistributionByUseful(dfReviews: DataFrame) : Unit = {
    val reviewDistribution = dfReviews
      .groupBy("stars")
      .agg(
        count("*").alias("nb_reviews"),
        sum("useful").alias("nb_useful")
      )
      .orderBy("nb_useful")

    reviewDistribution.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> REVIEW_DISTRIBUTION_BY_USEFUL_TABLE))
      .mode("overwrite")
      .save()
  }

}
