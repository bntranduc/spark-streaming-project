import org.apache.spark.sql.{SparkSession, DataFrame}
import Config._

object UpdateDatabse {

    def updateReviewTable(spark: SparkSession, batchDF: DataFrame): Unit =  {
      val df_review_db = spark.read
        .format("jdbc")
        .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
        .load()
        .select("review_id")

        val new_reviews = batchDF
          .join(df_review_db, Seq("review_id"), "left_anti").distinct()

        new_reviews.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .mode("append")
          .save()
    }

    def updateUserTable(spark: SparkSession, usersDF: DataFrame): Unit =  {
        val df_users_db = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> USER_TABLE))
          .load()
          .select("user_id")

        val df_reviews_db = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("user_id")

        val new_users_ids = df_reviews_db.select("user_id")
            .join(df_users_db, Seq("user_id"), "left_anti").distinct()

        val new_users = usersDF
          .join(new_users_ids, Seq("user_id"), "inner")

        new_users.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> USER_TABLE))
          .mode("append")
          .save()
        
        println("updateUserTable new_users =", new_users.count())

    }

    def updateBusinessTable(spark: SparkSession, businessDF: DataFrame): Unit =  {
        val df_business_db = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
          .load()
          .select("business_id")

        val df_reviews_db = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("business_id")

        val new_business_ids = df_reviews_db.select("business_id")
            .join(df_business_db, Seq("business_id"), "left_anti").distinct()

        val new_business = businessDF
          .join(new_business_ids, Seq("business_id"), "inner")

        new_business.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
          .mode("append")
          .save()
        
        println("updateBusinessTable new_business =", new_business.count())
    }

    def updateTopCategoriesTable(topCategories :DataFrame): Unit = {
        topCategories.write
        .format("jdbc")
        .options(DB_CONFIG + ("dbtable" -> TOP_CATEGORIES_TABLE))
        .mode("overwrite")
        .save()
    }

}