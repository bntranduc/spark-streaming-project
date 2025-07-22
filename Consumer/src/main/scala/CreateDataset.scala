// package com.example

// import org.apache.spark.sql.expressions.Window
// import org.apache.spark.sql.{DataFrame, SparkSession}
// import scala.annotation.tailrec
// import org.apache.spark.sql.functions._
// import scala.util.{Failure, Success, Try}
// import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, TOP_CATEGORIES_TABLE, USER_TABLE}
// import org.apache.spark.sql.functions._

// object CreateData {

//     val REVIW_COUNT = 10000
//     def main(args: Array[String]): Unit = {
        
//       val spark = SparkSession.builder()
//         .appName("Consumer")
//         .master("local[*]")
//         .config("spark.driver.memory", "4g")
//         .config("spark.sql.shuffle.partitions", "4")
//         .getOrCreate()

//       spark.sparkContext.setLogLevel("ERROR")
      
//           val reviewsPath = "/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset/yelp_academic_dataset_review.json"
//           val businessPath = "/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset/yelp_academic_dataset_business.json"  
//           val usersPath = "/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset/yelp_academic_dataset_user.json"

//           val allReviews = spark.read.json(reviewsPath).limit(REVIW_COUNT)
//           val allbusiness = spark.read.json(businessPath)
//           val allUsers = spark.read.json(usersPath)

//           println(s"allReviews count ${allReviews.count()}")
//           println(s"smple allReviews ${allReviews.show(2)}")

//           println(s"all allbusiness count ${allbusiness.count()}")
//           println(s"smple allbusiness ${allbusiness.show(2)}")

//           println(s"all allUsers count ${allUsers.count()}")
//           println(s"smple allUsers ${allUsers.show(2)}")


//           val uniqueBusinessIds = allReviews.select("business_id").distinct()
//           println(s"uniqueBusinessIds count ${uniqueBusinessIds.count()}")

//           val concernedBusiness = allbusiness
//             .join(uniqueBusinessIds, allbusiness("business_id") === uniqueBusinessIds("business_id"))
//             .drop(uniqueBusinessIds("business_id"))
//           println(s"concernedBusiness count ${concernedBusiness.count()}")
//           println(s"smple concernedBusiness ${concernedBusiness.show(2)}")

//           val uniqueUserIds = allReviews.select("user_id").distinct()
//           println(s"uniqueUserIds count ${uniqueUserIds.count()}")

//           val concernedUsers = allUsers
//             .join(uniqueUserIds, allUsers("user_id") === uniqueUserIds("user_id"))
//             .drop(uniqueUserIds("user_id"))

//           println(s"concernedUsers count ${concernedUsers.count()}")
//           println(s"smple concernedUsers ${concernedUsers.show(2)}")


//           allReviews.coalesce(1).write.mode("overwrite").json("/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset_test/yelp_academic_dataset_review.json")
//           concernedBusiness.coalesce(1).write.mode("overwrite").json("/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset_test/yelp_academic_dataset_business.json")
//           concernedUsers.coalesce(1).write.mode("overwrite").json("/home/adam/Documents/esgi/spark_streaming/spark-streaming-project_samedi/yelp_dataset_test/yelp_academic_dataset_user.json")

//         println("finish saved !")
//       spark.streams.awaitAnyTermination()

//     }
// }
