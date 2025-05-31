import org.apache.spark.sql.{SparkSession, DataFrame}
import Config._

object UpdateDatabse {
    val dbOptions = Map(
      "url" -> DB_URL,
      "user" -> DB_USER,
      "password" -> DB_PASSWORD,
      "driver" -> DB_DRIVER
    )

    def updateUserTable(spark: SparkSession, new_reviews: DataFrame, usersDF: DataFrame) {
        val df_users_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> USER_TABLE))
          .load()
          .select("user_id")

        val new_users_ids = new_reviews.select("user_id")
            .join(df_users_db, Seq("user_id"), "left_anti").distinct()

        val new_users = usersDF
          .join(new_users_ids, Seq("user_id"), "inner")

        new_users.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> USER_TABLE))
          .mode("append")
          .save()
    }

    def updateBusinessTable(spark: SparkSession, new_reviews: DataFrame, businessDF: DataFrame) {
        val df_business_db = spark.read
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
          .load()
          .select("business_id")

        val new_business_ids = new_reviews.select("business_id")
            .join(df_business_db, Seq("business_id"), "left_anti").distinct()

        val new_business = businessDF
          .join(new_business_ids, Seq("business_id"), "inner")

        new_business.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
          .mode("append")
          .save()
    }

    def updateReviewTable(new_reviews: DataFrame) {
        new_reviews.write
          .format("jdbc")
          .options(dbOptions + ("dbtable" -> REVIEW_TABLE))
          .mode("append")
          .save()
    }

    def updateTopFunBusinessTable(spark: SparkSession, topBusiness: DataFrame): Unit = {
      topBusiness.write
        .format("jdbc")
        .options(dbOptions + ("dbtable" -> TOP_FUN_BUSINESS_TABLE))
        .mode("overwrite")
        .save()
    }

    def updateTopUsefullUserTable(spark: SparkSession, topUsers: DataFrame): Unit = {
      topUsers.write
        .format("jdbc")
        .options(dbOptions + ("dbtable" -> TOP_USEFULL_USER_TABLE))
        .mode("overwrite")
        .save()
    }
}