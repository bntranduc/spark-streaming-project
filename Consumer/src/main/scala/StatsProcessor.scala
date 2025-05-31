import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Config._

import UpdateDatabse.{
    updateTopFunBusinessTable,
    updateTopUsefullUserTable,
    updateMostFaithfulUsersTable,
    updateTopRatedByCategoryTable,
    updateTopPopularBusinessByMonth,
    updateTopPopularUserTable,
    updateApexPredatorUserTable,
    updateClosedBusinessRatingStatsTable,
    updateActivityEvolutionTable,
    updateEliteImpactTable
}

object StatsProcessor {
    
    val dbOptions = Map(
      "url" -> DB_URL,
      "user" -> DB_USER,
      "password" -> DB_PASSWORD,
      "driver" -> DB_DRIVER
    )

    def processTopFunBusiness(spark: SparkSession): Unit = {

        val df_review_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("business_id", "useful")

        val top10Useful = df_review_db
            .groupBy("business_id")
            .agg(sum("useful").alias("total_useful"))
            .orderBy(desc("total_useful"))
            .limit(10)

        val df_business_db = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
            .load()

        val top10Business = top10Useful
            .join(df_business_db, Seq("business_id"))
            .orderBy(desc("total_useful"))

        top10Business.show()

        updateTopFunBusinessTable(spark, top10Business)
    }

    def processTopUsefulUser(spark: SparkSession) {
        val allDatabaseReviews = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("user_id", "useful")

        val allDatabaseUsers = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> USER_TABLE))
            .load()

        val top10UsefulUserIDs = allDatabaseReviews
            .groupBy("user_id")
            .agg(sum("useful").alias("total_useful"))
            .orderBy(desc("total_useful"))
            .limit(10)

        val top10UsefulUsers = top10UsefulUserIDs
            .join(allDatabaseUsers, Seq("user_id"))
            .orderBy(desc("total_useful"))
            .limit(10)

        top10UsefulUsers.show()

        updateTopUsefullUserTable(spark, top10UsefulUsers)
    }

    def processMostFaithfulUsersPerBusiness(spark: SparkSession) {
        val allDatabaseReviews = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("business_id", "user_id")

        val fidelityDF = allDatabaseReviews
            .groupBy("business_id", "user_id")
            .agg(count("*").alias("review_count"))

        val windowByBusiness = Window.partitionBy("business_id").orderBy(desc("review_count"))

        val mostFaithfulUsers = fidelityDF
            .withColumn("rank", rank().over(windowByBusiness))
            .filter(col("rank") === 3)

        mostFaithfulUsers.show()

        updateMostFaithfulUsersTable(spark, mostFaithfulUsers)
    }

    def processTopRatedBusinessByCategory(spark: SparkSession): Unit = {
        val businessDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
            .load()
            .select("business_id", "name", "city", "state", "categories")

        val reviewDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("business_id", "review_id", "stars")

        val joinedDF = reviewDF.join(businessDF, Seq("business_id"))

        val explodedDF = joinedDF.withColumn("category", explode(split(col("categories"), ",\\s*")))

        val aggDF = explodedDF
            .groupBy("category", "business_id", "name", "city", "state")
            .agg(
                count("review_id").alias("review_count"),
                avg("stars").alias("average_stars")
            )

        val window = Window.partitionBy("category").orderBy(desc("review_count"), desc("average_stars"))

        val rankedDF = aggDF
            .withColumn("rank", rank().over(window))
            .filter(col("rank") <= 3)

        rankedDF.show()

        updateTopRatedByCategoryTable(spark, rankedDF)
    }

    def processTopPopularBusinessByMonth(spark: SparkSession): Unit = {
        val reviewDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("review_id", "business_id", "date")

        val reviewWithMonth = reviewDF
            .withColumn("year_month", date_format(col("date"), "yyyy-MM"))

        val countByMonth = reviewWithMonth
            .groupBy("year_month", "business_id")
            .agg(count("review_id").alias("review_count"))

        val windowByMonth = Window.partitionBy("year_month").orderBy(desc("review_count"))

        val ranked = countByMonth
            .withColumn("rank", rank().over(windowByMonth))
            .filter(col("rank") <= 3)

        ranked.show()

        updateTopPopularBusinessByMonth(spark, ranked)
    }

    def processTopPopularUsers(spark: SparkSession): Unit = {
        val userDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> USER_TABLE))
            .load()
            .select("user_id", "name", "fans", "friends")

        val userWithFriendsCount = userDF
            .withColumn("friends_count",
                when(col("friends").isNull || col("friends") === "", lit(0))
                .otherwise(size(split(col("friends"), ",")))
            )

        val popularityDF = userWithFriendsCount
            .select("user_id", "name", "fans", "friends_count")
            .withColumn("popularity_score", col("fans") + col("friends_count"))

        val window = Window.orderBy(desc("popularity_score"))

        val ranked = popularityDF
            .withColumn("rank", rank().over(window))
            .filter(col("rank") <= 10)

        ranked.show()

        updateTopPopularUserTable(spark, ranked)
    }

    def processApexPredatorUsers(spark: SparkSession): Unit = {
        val userDF = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> USER_TABLE))
          .load()
          .select("user_id", "name", "elite")
    
        val withEliteCount = userDF.withColumn("elite_years",
          when(col("elite").isNull || col("elite") === "", lit(0))
          .otherwise(size(split(col("elite"), ",")))
        )
    
        val maxEliteYears = withEliteCount.agg(max("elite_years").alias("max_years")).first().getInt(0)
    
        val apexUsers = withEliteCount
          .filter(col("elite_years") === maxEliteYears)
    
        apexUsers.show()
    
        updateApexPredatorUserTable(spark, apexUsers)
    }

    def processClosedBusinessRatingStats(spark: SparkSession): Unit = {
        val businessDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
            .load()
            .select("business_id", "is_open")

        val reviewDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("business_id", "review_id", "stars")

        val closedReviewDF = reviewDF
            .join(businessDF.filter(col("is_open") === 0), Seq("business_id"))

        val resultDF = closedReviewDF
            .agg(
                avg("stars").alias("average_stars"),
                count("review_id").alias("review_count")
            )

        resultDF.show()

        updateClosedBusinessRatingStatsTable(spark, resultDF)
    }

    def processActivityEvolution(spark: SparkSession): Unit = {
        val reviewDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("review_id", "date")
            .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
            .groupBy("year_month")
            .agg(count("review_id").alias("reviews_count"))

        val userDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> USER_TABLE))
            .load()
            .select("user_id", "yelping_since")
            .withColumn("year_month", date_format(col("yelping_since"), "yyyy-MM"))
            .groupBy("year_month")
            .agg(count("user_id").alias("users_count"))

        val businessDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
            .load()

        val businessWithDate = if (businessDF.columns.contains("date")) {
            businessDF
              .select("business_id", "date")
              .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
              .groupBy("year_month")
              .agg(count("business_id").alias("business_count"))
        } else {
            spark.createDataFrame(Seq.empty[(String, Long)]).toDF("year_month", "business_count")
        }

        val joined = reviewDF
            .join(userDF, Seq("year_month"), "full_outer")
            .join(businessWithDate, Seq("year_month"), "full_outer")
            .na.fill(0)

        joined.orderBy("year_month").show()

        updateActivityEvolutionTable(spark, joined)
    }

    def processEliteImpactOnRatings(spark: SparkSession): Unit = {
        val userDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> USER_TABLE))
            .load()
            .select("user_id", "elite")
    
        val reviewDF = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
            .load()
            .select("user_id", "stars")
    
        val joined = reviewDF
            .join(userDF, Seq("user_id"))
    
        val withEliteStatus = joined.withColumn(
            "elite_status",
            when(col("elite").isNull || col("elite") === "", "non-elite").otherwise("elite")
        )
    
        val result = withEliteStatus
            .groupBy("elite_status")
            .agg(
                avg("stars").alias("average_stars"),
                count("*").alias("review_count")
            )
    
        result.show()
    
        updateEliteImpactTable(spark, result)
    }

}