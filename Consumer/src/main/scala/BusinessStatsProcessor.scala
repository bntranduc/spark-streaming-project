import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE}
import UpdateDatabase.{updateBusinessTable, updateUserTable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, desc, explode, length, round, row_number, split, sum, trim}

object BusinessStatsProcessor {

  def processTopCategoriesPerRating(spark: SparkSession): Unit = {
    val businessDF = spark.read
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
      .load()
      .select("categories", "avg_stars")

    val explodedDF = businessDF
      .withColumn("category", explode(split(col("categories"), ",\\s*")))
      .filter(col("category").isNotNull && length(trim(col("category"))) > 0)
      .withColumn("rounded_rating", round(col("avg_stars")).cast("int"))

    val categoryStats = explodedDF
      .groupBy("rounded_rating", "category")
      .agg(count("*").alias("nb_occurrences"))

    val windowSpec = Window.partitionBy("rounded_rating").orderBy(desc("nb_occurrences"))

    val topCategories = categoryStats
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 10)
      .drop("rank")

    topCategories.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "top_categories_by_rating"))
      .mode("overwrite")
      .save()
  }

  def processBusinessLocationState(sparkSession: SparkSession): Unit = {

    val businessDF = sparkSession.read
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
      .load()
      .select("state", "rounded_rating")

    val groupedBusiness = businessDF
      .groupBy("state", "rounded_rating")
      .agg(count("*").alias("nbr_business"))
      .orderBy("state", "rounded_rating")

    groupedBusiness.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "business_by_location_by_state"))
      .mode("overwrite")
      .save()
  }

  def processBusinessState(spark: SparkSession, allBusiness: DataFrame): Unit = {
    val reviews = spark.read
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
      .load()
      .select("business_id", "stars", "useful", "funny", "cool")

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

    updateBusinessTable(businessStates)
  }

  def processRatingByOpenStatus(spark: SparkSession): Unit = {
    val businessDF = spark.read
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
      .load()
      .select("is_open", "rounded_rating")

    val avgRatingByStatus = businessDF
      .groupBy("is_open")
      .agg(
        count("*").alias("nbr_business"),
        avg("rounded_rating").alias("avg_rating")
      )
      .orderBy(desc("is_open"))

    avgRatingByStatus.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "business_by_status_table"))
      .mode("overwrite")
      .save()
  }

}
