import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Config._

import UpdateDatabse.{updateTopFunBusinessTable, updateTopUsefullUserTable, updateMostFaithfulUsersTable}

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

    def processTopUsefulBusinessByCategory(spark: SparkSession) {
        val allDatabaseBusiness = spark.read
        .format("jdbc")
        .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
        .load()

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
            .filter(col("rank") === 1)

        mostFaithfulUsers.show()
        updateMostFaithfulUsersTable(spark, mostFaithfulUsers)
    }
}