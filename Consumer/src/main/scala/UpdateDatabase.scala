package com.example

import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, USER_TABLE}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

  def updateUserTable(filteredUsers: DataFrame): Unit = {
    filteredUsers.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> USER_TABLE))
      .mode("append")
      .save()
  }

  def updateBusinessTable(filteredBusiness: DataFrame): Unit = {
    filteredBusiness.write
      .format("jdbc")
      .options(DB_CONFIG + ("dbtable" -> BUSINESS_TABLE))
      .mode("append")
      .save()
  }

}