import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Config._
import UpdateDatabase.{updateBusinessTable, updateReviewEvolutionTable, updateTopCategoriesTable, updateUserTable}
import org.apache.spark.sql.expressions.Window

object StatsProcessor {

  def processRatingByOpenStatus(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._

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

    avgRatingByStatus.show()

    avgRatingByStatus.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "business_by_status_table"))
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


    def processUsersStates(spark: SparkSession, allUsersDF: DataFrame): Unit = {
        val df_reviews_db = spark.read
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("user_id", "stars", "useful", "funny", "cool")

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
                avg("stars").alias("avg_stars")
            )
            .orderBy(desc("total_reviews"))

        updateUserTable(userStats)
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

    def processReviewEvolution(spark: SparkSession): Unit = {
        val df_review_db = spark.read
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("review_id", "date")

        val review_by_date = df_review_db
            .withColumn("formated_date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("parsed_date", to_date(col("formated_date")))
            .groupBy("parsed_date")
            .agg(count("review_id").alias("total_reviews"))
            .orderBy("parsed_date")
        
        updateReviewEvolutionTable(review_by_date)
    }

    def saveNoteStarsDistribution(spark: SparkSession): Unit = {
        val dfReviews = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("stars")

        val noteDistribution = dfReviews
          .groupBy("stars")
          .count()
          .withColumnRenamed("count", "nb_notes")
          .orderBy("stars")

        noteDistribution.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> NOTE_DISTRIBUTION_TABLE))
          .mode("overwrite")
          .save()
    }


  def processReviewDistributionByUseful(spark: SparkSession) : Unit = {
    val reviews = spark.read
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
      .load()
      .select("stars", "useful")

    val reviewDistribution = reviews
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

    // Compter combien de fois chaque catégorie apparaît par note (1 à 5)
    val categoryStats = explodedDF
      .groupBy("rounded_rating", "category")
      .agg(count("*").alias("nb_occurrences"))

    // Utiliser window pour garder le Top 10 par note
    val windowSpec = Window.partitionBy("rounded_rating").orderBy(desc("nb_occurrences"))

    val topCategories = categoryStats
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 10)
      .drop("rank")

    // Sauvegarder les résultats dans une table PostgreSQL
    topCategories.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> "top_categories_by_rating"))
      .mode("overwrite")
      .save()
  }

}