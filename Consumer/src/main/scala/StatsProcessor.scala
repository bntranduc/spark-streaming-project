import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import Config._

import UpdateDatabase.updateTopCategoriesTable

object StatsProcessor {

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