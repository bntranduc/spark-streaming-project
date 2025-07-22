package com.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import Config.DB_CONFIG

object CompetitiveAnalysis {
    def processBusinessProfiles(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
        // """Profils complets des entreprises pour l'analyse concurrentielle"""
        
        // Statistiques détaillées par entreprise
        val businessStats = reviewsDF
            .groupBy("business_id")
            .agg(
                count("*").alias("total_reviews"),
                avg("stars").alias("average_rating"),
                stddev("stars").alias("rating_stddev"),
                sum("useful").alias("total_useful"),
                sum("funny").alias("total_funny"),
                sum("cool").alias("total_cool"),
                max("date").alias("last_review_date"),
                min("date").alias("first_review_date")
            )
        
        // Tendance récente (3 derniers mois)
        val recentStats = reviewsDF
            .filter(col("date") >= date_sub(current_date(), 90))
            .groupBy("business_id")
            .agg(
                avg("stars").alias("recent_average"),
                count("*").alias("recent_reviews"),
                countDistinct("user_id").alias("recent_unique_users")
            )
        
        // Distribution des notes par entreprise
        val ratingDistribution = reviewsDF
            .groupBy("business_id", "stars")
            .agg(count("*").alias("count"))
            .groupBy("business_id")
            .agg(
                sum(when(col("stars") === 1, col("count")).otherwise(0)).alias("rating_1"),
                sum(when(col("stars") === 2, col("count")).otherwise(0)).alias("rating_2"),
                sum(when(col("stars") === 3, col("count")).otherwise(0)).alias("rating_3"),
                sum(when(col("stars") === 4, col("count")).otherwise(0)).alias("rating_4"),
                sum(when(col("stars") === 5, col("count")).otherwise(0)).alias("rating_5")
            )
        
        // Combiner toutes les données
        val businessProfiles = businessDF
            .join(businessStats, "business_id")
            .join(recentStats, Seq("business_id"), "left")
            .join(ratingDistribution, "business_id")
            .withColumn("average_rating", round(col("average_rating"), 2))
            .withColumn("recent_average", round(coalesce(col("recent_average"), col("average_rating")), 2))
            .withColumn("rating_stddev", round(coalesce(col("rating_stddev"), lit(0)), 2))
            .withColumn("recent_reviews", coalesce(col("recent_reviews"), lit(0)))
            .withColumn("recent_unique_users", coalesce(col("recent_unique_users"), lit(0)))
            .select(
                "business_id", "name", "address", "city", "state", "categories", "latitude", "longitude", "is_open",
                "total_reviews", "average_rating", "recent_average", "rating_stddev",
                "total_useful", "total_funny", "total_cool",
                "recent_reviews", "recent_unique_users",
                "rating_1", "rating_2", "rating_3", "rating_4", "rating_5",
                "last_review_date", "first_review_date"
            )
        
        // Sauvegarde en base
        businessProfiles.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "business_profiles"))
            .mode("overwrite")
            .save()
            
        businessProfiles
    }

    def processCompetitorMappings(spark: SparkSession, businessDF: DataFrame): DataFrame = {
        // """Mappings des concurrents potentiels par entreprise"""
        
        import spark.implicits._
        
        // Extraire la catégorie principale de chaque business
        val businessWithMainCategory = businessDF
            .filter(col("categories").isNotNull)
            .withColumn("category_array", split(col("categories"), ","))
            .withColumn("main_category", trim(col("category_array").getItem(0)))
            .filter(col("main_category") =!= "")
            .select("business_id", "name", "city", "state", "main_category", "categories")
        
        // Auto-jointure pour trouver les concurrents potentiels
        val businessAlias1 = businessWithMainCategory.alias("b1")
        val businessAlias2 = businessWithMainCategory.alias("b2")
        
        val competitorMappings = businessAlias1
            .join(businessAlias2,
                (col("b1.main_category") === col("b2.main_category")) &&
                (col("b1.business_id") =!= col("b2.business_id")) &&
                (
                    (col("b1.city") === col("b2.city") && col("b1.state") === col("b2.state")) || // Même ville
                    (col("b1.state") === col("b2.state")) // Même état
                )
            ).select(
                col("b1.business_id").alias("target_business_id"),
                col("b1.name").alias("target_name"),
                col("b1.city").alias("target_city"),
                col("b1.state").alias("target_state"),
                col("b1.main_category").alias("target_category"),
                col("b2.business_id").alias("competitor_business_id"),
                col("b2.name").alias("competitor_name"),
                col("b2.city").alias("competitor_city"),
                col("b2.state").alias("competitor_state"),
                when(col("b1.city") === col("b2.city"), true).otherwise(false).alias("is_same_city")
            )
        
        // Limiter le nombre de concurrents par entreprise pour éviter l'explosion des données
        val windowSpec = Window.partitionBy("target_business_id").orderBy(
            desc("is_same_city"), // Prioriser les concurrents de la même ville
            col("competitor_name") // Ordre alphabétique pour la consistance
        )
        
        val limitedCompetitorMappings = competitorMappings
            .withColumn("rank", row_number().over(windowSpec))
            .filter(col("rank") <= 50) // Maximum 50 concurrents par entreprise
            .drop("rank")
        
        // Sauvegarde en base
        limitedCompetitorMappings.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "competitor_mappings"))
            .mode("overwrite")
            .save()
            
        limitedCompetitorMappings
    }

    def processCompetitiveAnalysis(spark: SparkSession, businessProfilesDF: DataFrame, competitorMappingsDF: DataFrame): DataFrame = {
        // """Analyse concurrentielle détaillée"""
        
        // Joindre les profils des entreprises avec leurs concurrents
        val competitiveData = competitorMappingsDF
            .join(businessProfilesDF.alias("target"), 
                col("target_business_id") === col("target.business_id"))
            .join(businessProfilesDF.alias("competitor"), 
                col("competitor_business_id") === col("competitor.business_id"))
            .select(
                col("target_business_id"),
                col("target_name"),
                col("target_city"),
                col("target_state"),
                col("target_category"),
                col("target.total_reviews").alias("target_total_reviews"),
                col("target.average_rating").alias("target_average_rating"),
                col("target.recent_average").alias("target_recent_average"),
                col("competitor_business_id"),
                col("competitor_name"),
                col("competitor_city"),
                col("competitor_state"),
                col("is_same_city"),
                col("competitor.total_reviews").alias("competitor_total_reviews"),
                col("competitor.average_rating").alias("competitor_average_rating"),
                col("competitor.recent_average").alias("competitor_recent_average"),
                col("competitor.rating_1").alias("competitor_rating_1"),
                col("competitor.rating_2").alias("competitor_rating_2"),
                col("competitor.rating_3").alias("competitor_rating_3"),
                col("competitor.rating_4").alias("competitor_rating_4"),
                col("competitor.rating_5").alias("competitor_rating_5")
            )
        
        // Calculs de positionnement concurrentiel
        val windowSpecRating = Window.partitionBy("target_business_id")
        val windowSpecPopularity = Window.partitionBy("target_business_id")
        
        val competitiveAnalysis = competitiveData
            .withColumn("better_rating_count", 
                sum(when(col("target_average_rating") > col("competitor_average_rating"), 1).otherwise(0))
                .over(windowSpecRating))
            .withColumn("total_competitors_rating", count("*").over(windowSpecRating))
            .withColumn("better_popularity_count", 
                sum(when(col("target_total_reviews") > col("competitor_total_reviews"), 1).otherwise(0))
                .over(windowSpecPopularity))
            .withColumn("total_competitors_popularity", count("*").over(windowSpecPopularity))
            .withColumn("rating_percentile", 
                round((col("better_rating_count").cast("double") / col("total_competitors_rating")) * 100, 1))
            .withColumn("popularity_percentile", 
                round((col("better_popularity_count").cast("double") / col("total_competitors_popularity")) * 100, 1))
            .withColumn("avg_percentile", 
                round((col("rating_percentile") + col("popularity_percentile")) / 2, 1))
            .withColumn("market_position",
                when(col("avg_percentile") >= 80, "Leader du marché")
                .when(col("avg_percentile") >= 60, "Bien positionné")
                .when(col("avg_percentile") >= 40, "Position moyenne")
                .otherwise("À améliorer")
            )
        
        // Sauvegarde en base
        competitiveAnalysis.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "competitive_analysis"))
            .mode("overwrite")
            .save()
            
        competitiveAnalysis
    }

    def processMarketPositioning(spark: SparkSession, competitiveAnalysisDF: DataFrame): DataFrame = {
        // """Calcul du positionnement sur le marché par entreprise"""
        
        val marketPositioning = competitiveAnalysisDF
            .groupBy(
                "target_business_id", "target_name", "target_city", "target_state", "target_category",
                "target_total_reviews", "target_average_rating", "target_recent_average"
            )
            .agg(
                first("rating_percentile").alias("rating_percentile"),
                first("popularity_percentile").alias("popularity_percentile"),
                first("avg_percentile").alias("avg_percentile"),
                first("market_position").alias("market_position"),
                count("*").alias("total_competitors"),
                sum(when(col("is_same_city"), 1).otherwise(0)).alias("same_city_competitors"),
                max("competitor_average_rating").alias("best_competitor_rating"),
                max("competitor_total_reviews").alias("most_popular_competitor_reviews"),
                avg("competitor_average_rating").alias("avg_competitor_rating"),
                avg("competitor_total_reviews").alias("avg_competitor_reviews")
            )
            .withColumn("avg_competitor_rating", round(col("avg_competitor_rating"), 2))
            .withColumn("avg_competitor_reviews", round(col("avg_competitor_reviews"), 0))
            
        // Identifier les forces et faiblesses
        val positioningWithInsights = marketPositioning
            .withColumn("strengths", 
            concat_ws(", ",
                when(col("rating_percentile") >= 70, lit("Excellence des notes")).otherwise(lit("")),
                when(col("popularity_percentile") >= 70, lit("Forte popularité")).otherwise(lit("")),
                when(col("same_city_competitors") < 5, lit("Faible concurrence locale")).otherwise(lit(""))
            )
            )
            .withColumn("weaknesses",
                concat_ws(", ",
                when(col("rating_percentile") <= 30, lit("Notes inférieures à la moyenne")).otherwise(lit("")),
                when(col("popularity_percentile") <= 30, lit("Faible visibilité")).otherwise(lit("")),
                when(col("same_city_competitors") > 20, lit("Forte concurrence locale")).otherwise(lit(""))
            )
            )
            .withColumn("strengths", regexp_replace(col("strengths"), "^, |, $", ""))
            .withColumn("weaknesses", regexp_replace(col("weaknesses"), "^, |, $", ""))
        
        // Sauvegarde en base
        positioningWithInsights.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "market_positioning"))
            .mode("overwrite")
            .save()
            
        positioningWithInsights
    }

    def processDetailedComparisons(spark: SparkSession, competitiveAnalysisDF: DataFrame): DataFrame = {
        // """Comparaisons détaillées avec les top concurrents"""
        
        // Sélectionner les 10 meilleurs concurrents par entreprise cible
        val windowSpec = Window.partitionBy("target_business_id")
            .orderBy(desc("competitor_average_rating"), desc("competitor_total_reviews"))
        
        val topCompetitors = competitiveAnalysisDF
            .withColumn("competitor_rank", row_number().over(windowSpec))
            .filter(col("competitor_rank") <= 10) // Top 10 concurrents
            .select(
                "target_business_id", "target_name",
                "competitor_business_id", "competitor_name", "competitor_city", "competitor_state",
                "is_same_city", "competitor_rank",
                "competitor_total_reviews", "competitor_average_rating", "competitor_recent_average",
                "competitor_rating_1", "competitor_rating_2", "competitor_rating_3", 
                "competitor_rating_4", "competitor_rating_5"
            )
        
        // Sauvegarde en base
        topCompetitors.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "detailed_comparisons"))
            .mode("overwrite")
            .save()
            
        topCompetitors
    }

    def processMarketShare(spark: SparkSession, competitiveAnalysisDF: DataFrame): DataFrame = {
        // """Calcul des parts de marché par zone géographique et catégorie"""
        
        // Parts de marché basées sur le volume d'avis
        val marketShareData = competitiveAnalysisDF
            .select("target_business_id", "target_name", "target_city", "target_state", "target_category", "target_total_reviews")
            .union(
                competitiveAnalysisDF.select(
                    col("competitor_business_id").alias("target_business_id"),
                    col("competitor_name").alias("target_name"),
                    col("competitor_city").alias("target_city"),
                    col("competitor_state").alias("target_state"),
                    col("target_category"),
                    col("competitor_total_reviews").alias("target_total_reviews")
                )
            )
            .distinct()
        
        // Calcul des totaux par marché (ville + catégorie)
        val marketTotals = marketShareData
            .groupBy("target_city", "target_state", "target_category")
            .agg(
                sum("target_total_reviews").alias("total_market_reviews")
                )
        
        // Calcul des parts de marché
        val marketShares = marketShareData
            .join(marketTotals, 
                Seq("target_city", "target_state", "target_category"))
            .withColumn("market_share_pct", 
                round((col("target_total_reviews").cast("double") / col("total_market_reviews")) * 100, 2))
            .filter(col("total_market_reviews") >= 100) // Filtrer les petits marchés
            .orderBy(col("target_city"), col("target_state"), col("target_category"), desc("market_share_pct"))
        
        // Sauvegarde en base
        marketShares.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "market_shares"))
            .mode("overwrite")
            .save()
            
        marketShares
    }

    def processCompetitiveInsights(spark: SparkSession, marketPositioningDF: DataFrame): DataFrame = {
    // """Génération d'insights concurrentiels automatiques"""
    
    val competitiveInsights = marketPositioningDF
        .withColumn("primary_insight",
            when(col("rating_percentile") >= 70 && col("popularity_percentile") >= 70, 
                "Leader du marché avec excellence sur tous les fronts")
            .when(col("rating_percentile") >= 70 && col("popularity_percentile") < 50, 
                "Excellente qualité mais manque de visibilité")
            .when(col("rating_percentile") < 50 && col("popularity_percentile") >= 70, 
                "Forte visibilité mais qualité à améliorer")
            .when(col("same_city_competitors") < 5, 
                "Avantage concurrentiel géographique")
            .when(col("total_competitors") > 30, 
                "Marché très concurrentiel")
            .otherwise("Position à consolider")
        )
        .withColumn("recommended_action",
            when(col("rating_percentile") < 50, 
                "Priorité: Améliorer la satisfaction client et la qualité du service")
            .when(col("popularity_percentile") < 50, 
                "Priorité: Augmenter la visibilité et encourager les avis clients")
            .when(col("same_city_competitors") < 3, 
                "Opportunité: Dominer le marché local")
            .otherwise("Maintenir les standards et surveiller la concurrence")
        )
        .withColumn("competitive_advantage",
            when(col("target_average_rating") > col("avg_competitor_rating") + 0.5, 
                "Avantage qualité significatif")
            .when(col("target_total_reviews") > col("avg_competitor_reviews") * 2, 
                "Avantage popularité significatif")
            .when(col("same_city_competitors") <= col("total_competitors") * 0.3, 
                "Avantage géographique")
            .otherwise("Aucun avantage concurrentiel majeur identifié")
        )
    
        // Sauvegarde en base
        competitiveInsights.write
            .format("jdbc")
            .options(DB_CONFIG + ("dbtable" -> "competitive_insights"))
            .mode("overwrite")
            .save()
            
        competitiveInsights
    }

    def processAllCompetitiveAnalytics(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
        // """Traite toutes les analyses concurrentielles et les sauvegarde en base"""
        
        // Filtrer pour les entreprises avec un minimum d'activité
        val activeBusinessIds = reviewsDF
            .groupBy("business_id")
            .agg(count("*").alias("review_count"))
            .filter(col("review_count") >= 5) // Au moins 5 avis
            .select("business_id")
            
        val filteredBusiness = businessDF
            .join(activeBusinessIds, "business_id")
            .filter(col("categories").isNotNull) // Avoir des catégories pour l'analyse
            
        val filteredReviews = reviewsDF
            .join(activeBusinessIds, "business_id")

        println(">>>>>>>>>>>>>>>>>>>>>>> Traitement des analyses concurrentielles <<<<<<<<<<<<<<<<<<<<<<<<")
        
        // 1. Profils d'entreprises détaillés
        val businessProfiles = processBusinessProfiles(spark, businessDF, reviewsDF)
        println(s"1- Business profiles: ${businessProfiles.count()} entreprises")
        
        // 2. Mappings des concurrents
        val competitorMappings = processCompetitorMappings(spark, businessDF)
        println(s"2- Competitor mappings: ${competitorMappings.count()} relations")
        
        // 3. Analyse concurrentielle
        val competitiveAnalysis = processCompetitiveAnalysis(spark, businessProfiles, competitorMappings)
        println(s"3- Competitive analysis: ${competitiveAnalysis.count()} comparaisons")
        
        // 4. Positionnement sur le marché
        val marketPositioning = processMarketPositioning(spark, competitiveAnalysis)
        println(s"4- Market positioning: ${marketPositioning.count()} entreprises")
        
        // 5. Comparaisons détaillées
        val detailedComparisons = processDetailedComparisons(spark, competitiveAnalysis)
        println(s"5- Detailed comparisons: ${detailedComparisons.count()} comparaisons détaillées")
        
        // 6. Parts de marché
        val marketShares = processMarketShare(spark, competitiveAnalysis)
        println(s"6- Market shares: ${marketShares.count()} parts de marché")
        
        // 7. Insights concurrentiels
        val competitiveInsights = processCompetitiveInsights(spark, marketPositioning)
        println(s"7- Competitive insights: ${competitiveInsights.count()} insights")
        
        println(" ================ Toutes les analyses concurrentielles terminées et sauvegardées! ================")
    }

}