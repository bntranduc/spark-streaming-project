import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Config._

import UpdateDatabase.{
    updateReviewEvolutionTable,
    updateTopCategoriesTable
}

object StatsProcessor {
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
        
        updateReviewEvolutionTable(spark, review_by_date)
    }

    def processTopCategories(spark: SparkSession): Unit = {
        val businessDF = spark.read
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
            .load()
            .select("categories")

        val categoriesDF = businessDF
            .withColumn("category", explode(split(col("categories"), ",\\s*")))
            .filter(col("category").isNotNull)

        val topCategories = categoriesDF
            .groupBy("category")
            .count()
            .orderBy(desc("count"))
            .limit(10)

        updateTopCategoriesTable(topCategories)
    }
}