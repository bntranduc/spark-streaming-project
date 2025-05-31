import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import Config._

import UpdateDatabse.{updateTopFunBusinessTable}

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
        
        println(df_review_db.count())

        val top10Useful = df_review_db
            .groupBy("business_id")
            .agg(sum("useful").alias("total_useful"))
            .orderBy(desc("total_useful"))
            .limit(10)

        top10Useful.show()

        val df_business_db = spark.read
            .format("jdbc")
            .options(dbOptions + ("dbtable" -> BUSINESS_TABLE))
            .load()

        val top10BusinessInfo = top10Useful
            .join(df_business_db, Seq("business_id"))
            .orderBy(desc("total_useful"))
        
        updateTopFunBusinessTable(spark, top10BusinessInfo)
    }
}