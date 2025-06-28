import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, TOP_CATEGORIES_TABLE, USER_TABLE}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    def updateUserTable(users: DataFrame): Unit = {
        users.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> USER_TABLE))
            .mode("overwrite")
            .save()
    }

    def updateBusinessTable(business: DataFrame): Unit =  {
        business.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
            .mode("overwrite")
            .save()
    }

    def updateTopCategoriesTable(topCategories :DataFrame): Unit = {
        topCategories.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> TOP_CATEGORIES_TABLE))
            .mode("overwrite")
            .save()
    }
}