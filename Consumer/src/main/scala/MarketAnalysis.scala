// object MarketAnalysis {
    
//     // ================== AGGREGATIONS POUR ANALYSE DE MARCHÉ ==================
//     def processMarketLocations(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Analyse des localisations disponibles avec métriques"""
        
//         val locationStats = businessDF
//             .groupBy("state", "city")
//             .agg(
//             count("*").alias("business_count"),
//             countDistinct("business_id").alias("unique_businesses")
//             )
//             .filter(col("business_count") >= 10) // Minimum 10 entreprises
//             .withColumn("display_name", concat(col("city"), lit(", "), col("state")))
//             .orderBy(desc("business_count"))
        
//         // Enrichir avec données de reviews
//         val businessLocationReviews = businessDF
//             .select("business_id", "state", "city")
//             .join(reviewsDF, "business_id")
//             .groupBy("state", "city")
//             .agg(
//             count("*").alias("total_reviews"),
//             avg("stars").alias("avg_rating"),
//             countDistinct("user_id").alias("unique_reviewers")
//             )
        
//         val enrichedLocations = locationStats
//             .join(businessLocationReviews, Seq("state", "city"), "left")
//             .withColumn("avg_rating", round(coalesce(col("avg_rating"), lit(0)), 2))
//             .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
//             .withColumn("unique_reviewers", coalesce(col("unique_reviewers"), lit(0)))
//             .select("state", "city", "display_name", "business_count", "total_reviews", 
//                     "avg_rating", "unique_reviewers")
        
//         // Sauvegarde en base
//         enrichedLocations.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_locations"))
//             .mode("overwrite")
//             .save()
            
//         enrichedLocations
//     }

//     def processMarketCategories(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Analyse des catégories de marché"""
        
//         import spark.implicits._
        
//         // Exploser les catégories
//         val businessCategories = businessDF
//             .filter(col("categories").isNotNull)
//             .withColumn("category", explode(split(col("categories"), ",")))
//             .withColumn("category", trim(col("category")))
//             .filter(col("category") =!= "")
//             .select("business_id", "category", "state", "city")
        
//         // Compter les occurrences par catégorie
//         val categoryStats = businessCategories
//             .groupBy("category")
//             .agg(
//             count("*").alias("business_count"),
//             countDistinct("business_id").alias("unique_businesses")
//             )
//             .filter(col("business_count") >= 20) // Minimum 20 occurrences
        
//         // Enrichir avec données de reviews
//         val categoryReviews = businessCategories
//             .join(reviewsDF, "business_id")
//             .groupBy("category")
//             .agg(
//             count("*").alias("total_reviews"),
//             avg("stars").alias("avg_rating"),
//             stddev("stars").alias("rating_stddev"),
//             countDistinct("user_id").alias("unique_reviewers")
//             )
        
//         val enrichedCategories = categoryStats
//             .join(categoryReviews, "category")
//             .withColumn("avg_rating", round(col("avg_rating"), 2))
//             .withColumn("rating_stddev", round(coalesce(col("rating_stddev"), lit(0)), 2))
//             .withColumn("saturation", 
//             when(col("business_count") < 30, "Faible")
//             .when(col("business_count") < 100, "Moyenne")
//             .otherwise("Élevée")
//             )
//             .withColumn("opportunity_score", 
//             // Score d'opportunité basé sur demande vs concurrence vs qualité
//             round(
//                 (least(col("total_reviews") / 1000, lit(10)) + // Demande normalisée
//                 greatest(lit(0), lit(10) - col("business_count") / 10) + // Moins de concurrence = mieux
//                 greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
//                 2
//             )
//             )
//             .orderBy(desc("opportunity_score"))
        
//         // Sauvegarde en base
//         enrichedCategories.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_categories"))
//             .mode("overwrite")
//             .save()
            
//         enrichedCategories
//     }

//     def processMarketOverview(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Vue d'ensemble du marché par localisation et catégorie"""
        
//         // Analyse par localisation
//         val locationOverview = businessDF
//             .groupBy("state", "city")
//             .agg(
//             count("*").alias("total_businesses"),
//             sum(when(col("is_open") === 1, 1).otherwise(0)).alias("active_businesses")
//             )
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
        
//         // Enrichir avec données de reviews
//         val locationReviews = businessDF
//             .select("business_id", "state", "city")
//             .join(reviewsDF, "business_id")
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
//             .withColumn("is_recent", 
//             when(col("date") >= date_sub(current_date(), 365), 1).otherwise(0)
//             )
//             .groupBy("location_key", "state", "city")
//             .agg(
//             count("*").alias("total_reviews"),
//             avg("stars").alias("avg_market_rating"),
//             countDistinct("business_id").alias("businesses_with_reviews"),
//             sum("is_recent").alias("recent_reviews"),
//             countDistinct(when(col("is_recent") === 1, col("business_id"))).alias("recently_active_businesses")
//             )
        
//         val marketOverview = locationOverview
//             .join(locationReviews, Seq("location_key", "state", "city"), "left")
//             .withColumn("avg_market_rating", round(coalesce(col("avg_market_rating"), lit(0)), 2))
//             .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
//             .withColumn("recently_active_businesses", coalesce(col("recently_active_businesses"), lit(0)))
//             .withColumn("activity_rate", 
//             round((col("recently_active_businesses").cast("double") / col("total_businesses")) * 100, 1)
//             )
//             .withColumn("market_health",
//             when(col("avg_market_rating") >= 4.0 && col("activity_rate") >= 60, "Excellent")
//             .when(col("avg_market_rating") >= 3.5 && col("activity_rate") >= 40, "Bon")
//             .when(col("avg_market_rating") >= 3.0 && col("activity_rate") >= 20, "Moyen")
//             .otherwise("Difficile")
//             )
        
//         // Sauvegarde en base
//         marketOverview.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_overview"))
//             .mode("overwrite")
//             .save()
            
//         marketOverview
//     }

//     def processMarketSegments(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Analyse des segments de marché par localisation et catégorie"""
        
//         import spark.implicits._
        
//         // Exploser les catégories avec localisation
//         val businessCategoriesLocation = businessDF
//             .filter(col("categories").isNotNull)
//             .withColumn("category", explode(split(col("categories"), ",")))
//             .withColumn("category", trim(col("category")))
//             .filter(col("category") =!= "")
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
//             .select("business_id", "category", "location_key", "state", "city")
        
//         // Stats par segment (location + category)
//         val segmentStats = businessCategoriesLocation
//             .groupBy("location_key", "category", "state", "city")
//             .agg(
//             count("*").alias("business_count"),
//             countDistinct("business_id").alias("unique_businesses")
//             )
//             .filter(col("business_count") >= 5) // Minimum 5 entreprises par segment
        
//         // Enrichir avec reviews
//         val segmentReviews = businessCategoriesLocation
//             .join(reviewsDF, "business_id")
//             .groupBy("location_key", "category")
//             .agg(
//             count("*").alias("total_reviews"),
//             avg("stars").alias("avg_rating"),
//             countDistinct("user_id").alias("unique_reviewers"),
//             sum(when(col("date") >= date_sub(current_date(), 365), 1).otherwise(0)).alias("recent_reviews")
//             )
        
//         val marketSegments = segmentStats
//             .join(segmentReviews, Seq("location_key", "category"), "left")
//             .withColumn("avg_rating", round(coalesce(col("avg_rating"), lit(0)), 2))
//             .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
//             .withColumn("recent_reviews", coalesce(col("recent_reviews"), lit(0)))
//             .withColumn("saturation",
//             when(col("business_count") < 10, "Faible")
//             .when(col("business_count") < 30, "Moyenne")
//             .otherwise("Élevée")
//             )
//             .withColumn("opportunity_score",
//             round(
//                 (least(col("total_reviews") / 500, lit(10)) + // Demande locale
//                 greatest(lit(0), lit(10) - col("business_count") / 5) + // Concurrence locale
//                 greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
//                 2
//             )
//             )
//             .orderBy(desc("opportunity_score"))
        
//         // Sauvegarde en base
//         marketSegments.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_segments"))
//             .mode("overwrite")
//             .save()
            
//         marketSegments
//     }

//     def processMarketTemporalTrends(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Analyse des tendances temporelles du marché"""
        
//         // Tendances par trimestre et localisation
//         val quarterlyTrends = businessDF
//             .select("business_id", "state", "city")
//             .join(reviewsDF, "business_id")
//             .withColumn("quarter", concat(year(col("date")), lit("-Q"), quarter(col("date"))))
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
//             .groupBy("location_key", "quarter", "state", "city")
//             .agg(
//             avg("stars").alias("avg_rating"),
//             count("*").alias("review_count"),
//             countDistinct("business_id").alias("active_businesses"),
//             countDistinct("user_id").alias("unique_users")
//             )
//             .withColumn("avg_rating", round(col("avg_rating"), 2))
//             .orderBy("location_key", "quarter")
        
//         // Calcul des tendances (comparaison récente vs ancienne)
//         val windowSpec = Window.partitionBy("location_key").orderBy("quarter")
        
//         val trendsWithMetrics = quarterlyTrends
//             .withColumn("quarter_rank", row_number().over(windowSpec))
//             .withColumn("total_quarters", count("quarter").over(Window.partitionBy("location_key")))
        
//         val trendsSummary = trendsWithMetrics
//             .filter(col("total_quarters") >= 4) // Au moins 4 trimestres
//             .groupBy("location_key", "state", "city", "total_quarters")
//             .agg(
//             avg(when(col("quarter_rank") <= 3, col("avg_rating"))).alias("early_avg_rating"),
//             avg(when(col("quarter_rank") > col("total_quarters") - 3, col("avg_rating"))).alias("recent_avg_rating"),
//             avg(when(col("quarter_rank") <= 3, col("review_count"))).alias("early_avg_activity"),
//             avg(when(col("quarter_rank") > col("total_quarters") - 3, col("review_count"))).alias("recent_avg_activity")
//             )
//             .withColumn("rating_trend", 
//             round(col("recent_avg_rating") - col("early_avg_rating"), 2)
//             )
//             .withColumn("activity_trend", 
//             round(col("recent_avg_activity") - col("early_avg_activity"), 1)
//             )
//             .withColumn("trend_interpretation",
//             when(col("rating_trend") > 0.1 && col("activity_trend") > 50, "Marché en croissance avec amélioration de la qualité")
//             .when(col("rating_trend") > 0.1 && col("activity_trend") < -50, "Amélioration de la qualité mais baisse d'activité")
//             .when(col("rating_trend") < -0.1 && col("activity_trend") > 50, "Croissance d'activité mais dégradation de la qualité")
//             .when(col("rating_trend") < -0.1 && col("activity_trend") < -50, "Marché en déclin avec dégradation de la qualité")
//             .when(abs(col("rating_trend")) <= 0.1 && col("activity_trend") > 50, "Marché en croissance avec qualité stable")
//             .when(abs(col("rating_trend")) <= 0.1 && col("activity_trend") < -50, "Baisse d'activité avec qualité stable")
//             .otherwise("Marché stable")
//             )
        
//         // Sauvegarde des données détaillées
//         quarterlyTrends.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_quarterly_trends"))
//             .mode("overwrite")
//             .save()
        
//         // Sauvegarde du résumé des tendances
//         trendsSummary.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_trends_summary"))
//             .mode("overwrite")
//             .save()
            
//         trendsSummary
//     }

//     def processMarketOpportunities(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Identification des opportunités de marché"""
        
//         import spark.implicits._
        
//         // Combiner les analyses de segments pour identifier les opportunités
//         val businessCategoriesLocation = businessDF
//             .filter(col("categories").isNotNull)
//             .withColumn("category", explode(split(col("categories"), ",")))
//             .withColumn("category", trim(col("category")))
//             .filter(col("category") =!= "")
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
//             .select("business_id", "category", "location_key", "state", "city")
        
//         // Analyser chaque combinaison location-category
//         val opportunities = businessCategoriesLocation
//             .join(reviewsDF, "business_id")
//             .groupBy("location_key", "category", "state", "city")
//             .agg(
//             count("*").alias("total_reviews"),
//             avg("stars").alias("avg_rating"),
//             countDistinct("business_id").alias("business_count"),
//             countDistinct("user_id").alias("unique_customers")
//             )
//             .withColumn("avg_rating", round(col("avg_rating"), 2))
//             .withColumn("opportunity_score",
//             round(
//                 (least(col("total_reviews") / 500, lit(10)) + // Demande
//                 greatest(lit(0), lit(10) - col("business_count") / 5) + // Concurrence
//                 greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
//                 2
//             )
//             )
//             .withColumn("opportunity_type",
//             when(col("opportunity_score") >= 7, "Forte opportunité")
//             .when(col("opportunity_score") >= 5, "Opportunité modérée")
//             .when(col("avg_rating") < 3.5 && col("business_count") >= 10, "Opportunité qualité")
//             .otherwise("Potentiel limité")
//             )
//             .withColumn("description",
//             when(col("opportunity_score") >= 7, 
//                 concat(lit("Segment "), col("category"), lit(" avec forte demande et faible concurrence")))
//             .when(col("opportunity_score") >= 5,
//                 concat(lit("Segment "), col("category"), lit(" avec potentiel d'amélioration")))
//             .when(col("avg_rating") < 3.5 && col("business_count") >= 10,
//                 concat(lit("Segment "), col("category"), lit(" avec lacune qualité à combler")))
//             .otherwise(concat(lit("Segment "), col("category"), lit(" saturé ou peu demandé")))
//             )
//             .filter(col("opportunity_score") >= 4) // Filtrer les vraies opportunités
//             .orderBy(desc("opportunity_score"))
        
//         // Sauvegarde en base
//         opportunities.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_opportunities"))
//             .mode("overwrite")
//             .save()
            
//         opportunities
//     }

//     def processMarketRatingDistribution(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
//         """Distribution des notes par marché (localisation)"""
        
//         val ratingDistribution = businessDF
//             .select("business_id", "state", "city")
//             .join(reviewsDF, "business_id")
//             .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
//             .groupBy("location_key", "state", "city", "stars")
//             .agg(count("*").alias("review_count"))
//             .withColumn("rating", col("stars").cast("int"))
//             .select("location_key", "state", "city", "rating", "review_count")
//             .orderBy("location_key", "rating")
        
//         // Sauvegarde en base
//         ratingDistribution.write
//             .format("jdbc")
//             .options(DB_CONFIG + ("dbtable" -> "market_rating_distribution"))
//             .mode("overwrite")
//             .save()
            
//         ratingDistribution
//     }

//     // ================== FONCTION PRINCIPALE ==================

//     def processAllMarketAnalytics(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
//         """Traite toutes les analyses de marché et les sauvegarde en base"""
        
//         println("🔄 Traitement des analyses de marché...")
        
//         // 1. Localisations disponibles
//         val locations = processMarketLocations(spark, businessDF, reviewsDF)
//         println(s"✅ Market locations: ${locations.count()} localisations")
        
//         // 2. Catégories de marché
//         val categories = processMarketCategories(spark, businessDF, reviewsDF)
//         println(s"✅ Market categories: ${categories.count()} catégories")
        
//         // 3. Vue d'ensemble du marché
//         val overview = processMarketOverview(spark, businessDF, reviewsDF)
//         println(s"✅ Market overview: ${overview.count()} marchés")
        
//         // 4. Segments de marché
//         val segments = processMarketSegments(spark, businessDF, reviewsDF)
//         println(s"✅ Market segments: ${segments.count()} segments")
        
//         // 5. Tendances temporelles
//         val trends = processMarketTemporalTrends(spark, businessDF, reviewsDF)
//         println(s"✅ Market trends: ${trends.count()} analyses de tendances")
        
//         // 6. Opportunités de marché
//         val opportunities = processMarketOpportunities(spark, businessDF, reviewsDF)
//         println(s"✅ Market opportunities: ${opportunities.count()} opportunités")
        
//         // 7. Distribution des notes par marché
//         val ratingDist = processMarketRatingDistribution(spark, businessDF, reviewsDF)
//         println(s"✅ Market rating distribution: ${ratingDist.count()} distributions")
        
//         println("🎉 Toutes les analyses de marché terminées et sauvegardées!")
//     }

//     // ================== INTEGRATION DANS VOTRE PIPELINE ==================

//     def integrateMarketAnalysisInPipeline(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
//         """Intégration dans votre pipeline existant"""
        
//         // Filtrer pour ne traiter que les données actives (avec reviews récents)
//         val activeBusinessIds = reviewsDF
//             .filter(col("date") >= date_sub(current_date(), 730)) // 2 ans
//             .select("business_id").distinct()
            
//         val filteredBusiness = businessDF.join(activeBusinessIds, "business_id")
//         val activeReviews = reviewsDF.filter(col("date") >= date_sub(current_date(), 730))
        
//         // Traitement des analyses de marché
//         processAllMarketAnalytics(spark, filteredBusiness, activeReviews)
//     }
// }