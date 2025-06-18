import org.apache.spark.sql.{DataFrame, SparkSession}
import Config._
import org.apache.spark.sql.functions.{avg, col, count, desc, lit, sum}

object UpdateDatabase {

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
    }

    def updateBusinessTable(spark: SparkSession, businessDF: DataFrame): Unit =  {
        // 1. Charger les reviews
        val reviews = spark.read
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> REVIEW_TABLE))
          .load()
          .select("business_id", "stars", "useful", "funny", "cool")  // Ajoute "cool" si tu veux l’agréger

        // 2. Calcul des métriques par business
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
          .orderBy(desc("total_reviews"))

        // 4. Join entre business et statistiques reviews
        val enrichedBusiness = businessDF
          .join(reviewStats, Seq("business_id"), "inner")

        // 5. Sauvegarde dans la base
        enrichedBusiness.write
          .format("jdbc")
          .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
          .mode("overwrite")
          .save()
    }

    def updateReviewEvolutionTable(spark: SparkSession, review_by_date: DataFrame): Unit = {
        review_by_date.write
        .format("jdbc")
        .options(DB_CONFIG + ("dbtable" -> REVIEW_EVOLUTION_TABLE))
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