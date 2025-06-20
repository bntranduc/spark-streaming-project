import Config.{DB_CONFIG, REVIEW_TABLE}
import UpdateDatabase.updateUserTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc, sum}

object UserStatsProcessor {

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
}
