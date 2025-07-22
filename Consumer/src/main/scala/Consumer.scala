package com.example
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.annotation.tailrec
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}
import Config.{BUSINESS_TABLE, DB_CONFIG, REVIEW_TABLE, USER_TABLE}
import org.apache.spark.sql.functions._

import DataSourceReader.loadOrCreateArtefactSafe
import Config.{
  BOOTSTRAP_SERVER,BUSINESS_ARTEFACT_PATH,BUSINESS_JSON_PATH,BUSINESS_SCHEMA,
  REVIEW_SCHEMA, REVIEW_TOPIC, USER_ARTEFACT_PATH, USER_JSON_PATH, USER_SCHEMA, REVIEW_JSON_PATH, REVIEW_ARTEFACT_PATH
}
import UpdateDatabase.{
  updateReviewTable,
  updateUserTable,
  updateBusinessTable
}

import  BusinessAnalytics.processAllBusinessAnalytics

object Consumer {
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

// ================== AGGREGATIONS POUR ANALYSE CONCURRENTIELLE ==================

def processBusinessProfiles(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Profils complets des entreprises pour l'analyse concurrentielle"""
  
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
  """Mappings des concurrents potentiels par entreprise"""
  
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
    )
    .select(
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
  """Analyse concurrentielle détaillée"""
  
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
  """Calcul du positionnement sur le marché par entreprise"""
  
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
  """Comparaisons détaillées avec les top concurrents"""
  
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
  """Calcul des parts de marché par zone géographique et catégorie"""
  
  // Parts de marché basées sur le volume d'avis
  val marketShareData = competitiveAnalysisDF
    .select("target_business_id", "target_name", "target_city", "target_state", 
            "target_category", "target_total_reviews")
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
    .agg(sum("target_total_reviews").alias("total_market_reviews"))
  
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
  """Génération d'insights concurrentiels automatiques"""
  
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

// ================== FONCTION PRINCIPALE ==================

def processAllCompetitiveAnalytics(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
  """Traite toutes les analyses concurrentielles et les sauvegarde en base"""
  
  println("🔄 Traitement des analyses concurrentielles...")
  
  // 1. Profils d'entreprises détaillés
  val businessProfiles = processBusinessProfiles(spark, businessDF, reviewsDF)
  println(s"✅ Business profiles: ${businessProfiles.count()} entreprises")
  
  // 2. Mappings des concurrents
  val competitorMappings = processCompetitorMappings(spark, businessDF)
  println(s"✅ Competitor mappings: ${competitorMappings.count()} relations")
  
  // 3. Analyse concurrentielle
  val competitiveAnalysis = processCompetitiveAnalysis(spark, businessProfiles, competitorMappings)
  println(s"✅ Competitive analysis: ${competitiveAnalysis.count()} comparaisons")
  
  // 4. Positionnement sur le marché
  val marketPositioning = processMarketPositioning(spark, competitiveAnalysis)
  println(s"✅ Market positioning: ${marketPositioning.count()} entreprises")
  
  // 5. Comparaisons détaillées
  val detailedComparisons = processDetailedComparisons(spark, competitiveAnalysis)
  println(s"✅ Detailed comparisons: ${detailedComparisons.count()} comparaisons détaillées")
  
  // 6. Parts de marché
  val marketShares = processMarketShare(spark, competitiveAnalysis)
  println(s"✅ Market shares: ${marketShares.count()} parts de marché")
  
  // 7. Insights concurrentiels
  val competitiveInsights = processCompetitiveInsights(spark, marketPositioning)
  println(s"✅ Competitive insights: ${competitiveInsights.count()} insights")
  
  println("🎉 Toutes les analyses concurrentielles terminées et sauvegardées!")
}

// ================== INTEGRATION DANS VOTRE PIPELINE ==================

def integrateCompetitiveAnalysisInPipeline(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
  """Intégration dans votre pipeline existant"""
  
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
  
  // Traitement des analyses concurrentielles
  processAllCompetitiveAnalytics(spark, filteredBusiness, filteredReviews)
}

  
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

// ================== AGGREGATIONS POUR ANALYSE DE MARCHÉ ==================

def processMarketLocations(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Analyse des localisations disponibles avec métriques"""
  
  val locationStats = businessDF
    .groupBy("state", "city")
    .agg(
      count("*").alias("business_count"),
      countDistinct("business_id").alias("unique_businesses")
    )
    .filter(col("business_count") >= 10) // Minimum 10 entreprises
    .withColumn("display_name", concat(col("city"), lit(", "), col("state")))
    .orderBy(desc("business_count"))
  
  // Enrichir avec données de reviews
  val businessLocationReviews = businessDF
    .select("business_id", "state", "city")
    .join(reviewsDF, "business_id")
    .groupBy("state", "city")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("avg_rating"),
      countDistinct("user_id").alias("unique_reviewers")
    )
  
  val enrichedLocations = locationStats
    .join(businessLocationReviews, Seq("state", "city"), "left")
    .withColumn("avg_rating", round(coalesce(col("avg_rating"), lit(0)), 2))
    .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
    .withColumn("unique_reviewers", coalesce(col("unique_reviewers"), lit(0)))
    .select("state", "city", "display_name", "business_count", "total_reviews", 
            "avg_rating", "unique_reviewers")
  
  // Sauvegarde en base
  enrichedLocations.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_locations"))
    .mode("overwrite")
    .save()
    
  enrichedLocations
}

def processMarketCategories(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Analyse des catégories de marché"""
  
  import spark.implicits._
  
  // Exploser les catégories
  val businessCategories = businessDF
    .filter(col("categories").isNotNull)
    .withColumn("category", explode(split(col("categories"), ",")))
    .withColumn("category", trim(col("category")))
    .filter(col("category") =!= "")
    .select("business_id", "category", "state", "city")
  
  // Compter les occurrences par catégorie
  val categoryStats = businessCategories
    .groupBy("category")
    .agg(
      count("*").alias("business_count"),
      countDistinct("business_id").alias("unique_businesses")
    )
    .filter(col("business_count") >= 20) // Minimum 20 occurrences
  
  // Enrichir avec données de reviews
  val categoryReviews = businessCategories
    .join(reviewsDF, "business_id")
    .groupBy("category")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("avg_rating"),
      stddev("stars").alias("rating_stddev"),
      countDistinct("user_id").alias("unique_reviewers")
    )
  
  val enrichedCategories = categoryStats
    .join(categoryReviews, "category")
    .withColumn("avg_rating", round(col("avg_rating"), 2))
    .withColumn("rating_stddev", round(coalesce(col("rating_stddev"), lit(0)), 2))
    .withColumn("saturation", 
      when(col("business_count") < 30, "Faible")
      .when(col("business_count") < 100, "Moyenne")
      .otherwise("Élevée")
    )
    .withColumn("opportunity_score", 
      // Score d'opportunité basé sur demande vs concurrence vs qualité
      round(
        (least(col("total_reviews") / 1000, lit(10)) + // Demande normalisée
         greatest(lit(0), lit(10) - col("business_count") / 10) + // Moins de concurrence = mieux
         greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
        2
      )
    )
    .orderBy(desc("opportunity_score"))
  
  // Sauvegarde en base
  enrichedCategories.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_categories"))
    .mode("overwrite")
    .save()
    
  enrichedCategories
}

def processMarketOverview(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Vue d'ensemble du marché par localisation et catégorie"""
  
  // Analyse par localisation
  val locationOverview = businessDF
    .groupBy("state", "city")
    .agg(
      count("*").alias("total_businesses"),
      sum(when(col("is_open") === 1, 1).otherwise(0)).alias("active_businesses")
    )
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
  
  // Enrichir avec données de reviews
  val locationReviews = businessDF
    .select("business_id", "state", "city")
    .join(reviewsDF, "business_id")
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
    .withColumn("is_recent", 
      when(col("date") >= date_sub(current_date(), 365), 1).otherwise(0)
    )
    .groupBy("location_key", "state", "city")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("avg_market_rating"),
      countDistinct("business_id").alias("businesses_with_reviews"),
      sum("is_recent").alias("recent_reviews"),
      countDistinct(when(col("is_recent") === 1, col("business_id"))).alias("recently_active_businesses")
    )
  
  val marketOverview = locationOverview
    .join(locationReviews, Seq("location_key", "state", "city"), "left")
    .withColumn("avg_market_rating", round(coalesce(col("avg_market_rating"), lit(0)), 2))
    .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
    .withColumn("recently_active_businesses", coalesce(col("recently_active_businesses"), lit(0)))
    .withColumn("activity_rate", 
      round((col("recently_active_businesses").cast("double") / col("total_businesses")) * 100, 1)
    )
    .withColumn("market_health",
      when(col("avg_market_rating") >= 4.0 && col("activity_rate") >= 60, "Excellent")
      .when(col("avg_market_rating") >= 3.5 && col("activity_rate") >= 40, "Bon")
      .when(col("avg_market_rating") >= 3.0 && col("activity_rate") >= 20, "Moyen")
      .otherwise("Difficile")
    )
  
  // Sauvegarde en base
  marketOverview.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_overview"))
    .mode("overwrite")
    .save()
    
  marketOverview
}

def processMarketSegments(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Analyse des segments de marché par localisation et catégorie"""
  
  import spark.implicits._
  
  // Exploser les catégories avec localisation
  val businessCategoriesLocation = businessDF
    .filter(col("categories").isNotNull)
    .withColumn("category", explode(split(col("categories"), ",")))
    .withColumn("category", trim(col("category")))
    .filter(col("category") =!= "")
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
    .select("business_id", "category", "location_key", "state", "city")
  
  // Stats par segment (location + category)
  val segmentStats = businessCategoriesLocation
    .groupBy("location_key", "category", "state", "city")
    .agg(
      count("*").alias("business_count"),
      countDistinct("business_id").alias("unique_businesses")
    )
    .filter(col("business_count") >= 5) // Minimum 5 entreprises par segment
  
  // Enrichir avec reviews
  val segmentReviews = businessCategoriesLocation
    .join(reviewsDF, "business_id")
    .groupBy("location_key", "category")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("avg_rating"),
      countDistinct("user_id").alias("unique_reviewers"),
      sum(when(col("date") >= date_sub(current_date(), 365), 1).otherwise(0)).alias("recent_reviews")
    )
  
  val marketSegments = segmentStats
    .join(segmentReviews, Seq("location_key", "category"), "left")
    .withColumn("avg_rating", round(coalesce(col("avg_rating"), lit(0)), 2))
    .withColumn("total_reviews", coalesce(col("total_reviews"), lit(0)))
    .withColumn("recent_reviews", coalesce(col("recent_reviews"), lit(0)))
    .withColumn("saturation",
      when(col("business_count") < 10, "Faible")
      .when(col("business_count") < 30, "Moyenne")
      .otherwise("Élevée")
    )
    .withColumn("opportunity_score",
      round(
        (least(col("total_reviews") / 500, lit(10)) + // Demande locale
         greatest(lit(0), lit(10) - col("business_count") / 5) + // Concurrence locale
         greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
        2
      )
    )
    .orderBy(desc("opportunity_score"))
  
  // Sauvegarde en base
  marketSegments.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_segments"))
    .mode("overwrite")
    .save()
    
  marketSegments
}

def processMarketTemporalTrends(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Analyse des tendances temporelles du marché"""
  
  // Tendances par trimestre et localisation
  val quarterlyTrends = businessDF
    .select("business_id", "state", "city")
    .join(reviewsDF, "business_id")
    .withColumn("quarter", concat(year(col("date")), lit("-Q"), quarter(col("date"))))
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
    .groupBy("location_key", "quarter", "state", "city")
    .agg(
      avg("stars").alias("avg_rating"),
      count("*").alias("review_count"),
      countDistinct("business_id").alias("active_businesses"),
      countDistinct("user_id").alias("unique_users")
    )
    .withColumn("avg_rating", round(col("avg_rating"), 2))
    .orderBy("location_key", "quarter")
  
  // Calcul des tendances (comparaison récente vs ancienne)
  val windowSpec = Window.partitionBy("location_key").orderBy("quarter")
  
  val trendsWithMetrics = quarterlyTrends
    .withColumn("quarter_rank", row_number().over(windowSpec))
    .withColumn("total_quarters", count("quarter").over(Window.partitionBy("location_key")))
  
  val trendsSummary = trendsWithMetrics
    .filter(col("total_quarters") >= 4) // Au moins 4 trimestres
    .groupBy("location_key", "state", "city", "total_quarters")
    .agg(
      avg(when(col("quarter_rank") <= 3, col("avg_rating"))).alias("early_avg_rating"),
      avg(when(col("quarter_rank") > col("total_quarters") - 3, col("avg_rating"))).alias("recent_avg_rating"),
      avg(when(col("quarter_rank") <= 3, col("review_count"))).alias("early_avg_activity"),
      avg(when(col("quarter_rank") > col("total_quarters") - 3, col("review_count"))).alias("recent_avg_activity")
    )
    .withColumn("rating_trend", 
      round(col("recent_avg_rating") - col("early_avg_rating"), 2)
    )
    .withColumn("activity_trend", 
      round(col("recent_avg_activity") - col("early_avg_activity"), 1)
    )
    .withColumn("trend_interpretation",
      when(col("rating_trend") > 0.1 && col("activity_trend") > 50, "Marché en croissance avec amélioration de la qualité")
      .when(col("rating_trend") > 0.1 && col("activity_trend") < -50, "Amélioration de la qualité mais baisse d'activité")
      .when(col("rating_trend") < -0.1 && col("activity_trend") > 50, "Croissance d'activité mais dégradation de la qualité")
      .when(col("rating_trend") < -0.1 && col("activity_trend") < -50, "Marché en déclin avec dégradation de la qualité")
      .when(abs(col("rating_trend")) <= 0.1 && col("activity_trend") > 50, "Marché en croissance avec qualité stable")
      .when(abs(col("rating_trend")) <= 0.1 && col("activity_trend") < -50, "Baisse d'activité avec qualité stable")
      .otherwise("Marché stable")
    )
  
  // Sauvegarde des données détaillées
  quarterlyTrends.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_quarterly_trends"))
    .mode("overwrite")
    .save()
  
  // Sauvegarde du résumé des tendances
  trendsSummary.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_trends_summary"))
    .mode("overwrite")
    .save()
    
  trendsSummary
}

def processMarketOpportunities(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Identification des opportunités de marché"""
  
  import spark.implicits._
  
  // Combiner les analyses de segments pour identifier les opportunités
  val businessCategoriesLocation = businessDF
    .filter(col("categories").isNotNull)
    .withColumn("category", explode(split(col("categories"), ",")))
    .withColumn("category", trim(col("category")))
    .filter(col("category") =!= "")
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
    .select("business_id", "category", "location_key", "state", "city")
  
  // Analyser chaque combinaison location-category
  val opportunities = businessCategoriesLocation
    .join(reviewsDF, "business_id")
    .groupBy("location_key", "category", "state", "city")
    .agg(
      count("*").alias("total_reviews"),
      avg("stars").alias("avg_rating"),
      countDistinct("business_id").alias("business_count"),
      countDistinct("user_id").alias("unique_customers")
    )
    .withColumn("avg_rating", round(col("avg_rating"), 2))
    .withColumn("opportunity_score",
      round(
        (least(col("total_reviews") / 500, lit(10)) + // Demande
         greatest(lit(0), lit(10) - col("business_count") / 5) + // Concurrence
         greatest(lit(0), (lit(5) - col("avg_rating")) * 2)) / 3, // Lacune qualité
        2
      )
    )
    .withColumn("opportunity_type",
      when(col("opportunity_score") >= 7, "Forte opportunité")
      .when(col("opportunity_score") >= 5, "Opportunité modérée")
      .when(col("avg_rating") < 3.5 && col("business_count") >= 10, "Opportunité qualité")
      .otherwise("Potentiel limité")
    )
    .withColumn("description",
      when(col("opportunity_score") >= 7, 
        concat(lit("Segment "), col("category"), lit(" avec forte demande et faible concurrence")))
      .when(col("opportunity_score") >= 5,
        concat(lit("Segment "), col("category"), lit(" avec potentiel d'amélioration")))
      .when(col("avg_rating") < 3.5 && col("business_count") >= 10,
        concat(lit("Segment "), col("category"), lit(" avec lacune qualité à combler")))
      .otherwise(concat(lit("Segment "), col("category"), lit(" saturé ou peu demandé")))
    )
    .filter(col("opportunity_score") >= 4) // Filtrer les vraies opportunités
    .orderBy(desc("opportunity_score"))
  
  // Sauvegarde en base
  opportunities.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_opportunities"))
    .mode("overwrite")
    .save()
    
  opportunities
}

def processMarketRatingDistribution(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): DataFrame = {
  """Distribution des notes par marché (localisation)"""
  
  val ratingDistribution = businessDF
    .select("business_id", "state", "city")
    .join(reviewsDF, "business_id")
    .withColumn("location_key", concat(col("city"), lit(", "), col("state")))
    .groupBy("location_key", "state", "city", "stars")
    .agg(count("*").alias("review_count"))
    .withColumn("rating", col("stars").cast("int"))
    .select("location_key", "state", "city", "rating", "review_count")
    .orderBy("location_key", "rating")
  
  // Sauvegarde en base
  ratingDistribution.write
    .format("jdbc")
    .options(DB_CONFIG + ("dbtable" -> "market_rating_distribution"))
    .mode("overwrite")
    .save()
    
  ratingDistribution
}

// ================== FONCTION PRINCIPALE ==================

def processAllMarketAnalytics(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
  """Traite toutes les analyses de marché et les sauvegarde en base"""
  
  println("🔄 Traitement des analyses de marché...")
  
  // 1. Localisations disponibles
  val locations = processMarketLocations(spark, businessDF, reviewsDF)
  println(s"✅ Market locations: ${locations.count()} localisations")
  
  // 2. Catégories de marché
  val categories = processMarketCategories(spark, businessDF, reviewsDF)
  println(s"✅ Market categories: ${categories.count()} catégories")
  
  // 3. Vue d'ensemble du marché
  val overview = processMarketOverview(spark, businessDF, reviewsDF)
  println(s"✅ Market overview: ${overview.count()} marchés")
  
  // 4. Segments de marché
  val segments = processMarketSegments(spark, businessDF, reviewsDF)
  println(s"✅ Market segments: ${segments.count()} segments")
  
  // 5. Tendances temporelles
  val trends = processMarketTemporalTrends(spark, businessDF, reviewsDF)
  println(s"✅ Market trends: ${trends.count()} analyses de tendances")
  
  // 6. Opportunités de marché
  val opportunities = processMarketOpportunities(spark, businessDF, reviewsDF)
  println(s"✅ Market opportunities: ${opportunities.count()} opportunités")
  
  // 7. Distribution des notes par marché
  val ratingDist = processMarketRatingDistribution(spark, businessDF, reviewsDF)
  println(s"✅ Market rating distribution: ${ratingDist.count()} distributions")
  
  println("🎉 Toutes les analyses de marché terminées et sauvegardées!")
}

// ================== INTEGRATION DANS VOTRE PIPELINE ==================

def integrateMarketAnalysisInPipeline(spark: SparkSession, businessDF: DataFrame, reviewsDF: DataFrame): Unit = {
  """Intégration dans votre pipeline existant"""
  
  // Filtrer pour ne traiter que les données actives (avec reviews récents)
  val activeBusinessIds = reviewsDF
    .filter(col("date") >= date_sub(current_date(), 730)) // 2 ans
    .select("business_id").distinct()
    
  val filteredBusiness = businessDF.join(activeBusinessIds, "business_id")
  val activeReviews = reviewsDF.filter(col("date") >= date_sub(current_date(), 730))
  
  // Traitement des analyses de marché
  processAllMarketAnalytics(spark, filteredBusiness, activeReviews)
}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "100")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val usersDF = loadOrCreateArtefactSafe(
        spark,
        USER_JSON_PATH,
        USER_ARTEFACT_PATH,
        USER_SCHEMA,
        Seq("user_id", "name", "fans", "elite", "friends", "yelping_since"),
      )
      val userDF = usersDF.withColumn("yelping_since", to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))

      val businessDF = loadOrCreateArtefactSafe(
        spark,
        BUSINESS_JSON_PATH,
        BUSINESS_ARTEFACT_PATH,
        BUSINESS_SCHEMA,
        Seq("business_id", "name", "city", "address" ,"latitude", "longitude","state", "categories", "is_Open"),
      )

      consumeKafkaTopic(spark, businessDF, userDF)
    } catch {
      case e: java.io.FileNotFoundException =>
        println(s"Fichier introuvable : ${e.getMessage}")
        e.printStackTrace()

      case e: org.apache.spark.sql.AnalysisException =>
        println(s"Erreur d'analyse Spark SQL : ${e.getMessage}")
        e.printStackTrace()

      case e: java.text.ParseException =>
        println(s"Erreur de parsing (date ou autre) : ${e.getMessage}")
        e.printStackTrace()

      case e: IllegalArgumentException =>
        println(s"Argument invalide : ${e.getMessage}")
        e.printStackTrace()

      case e: Exception =>
        println(s"Erreur inattendue : ${e.getMessage}")
        e.printStackTrace()
    }
    spark.streams.awaitAnyTermination()
  }

  private def consumeKafkaTopic(spark: SparkSession, businessDF: DataFrame, usersDF: DataFrame): Unit = {

    val maybeKafkaDF = tryConnect(spark, attempt=1, retries=5, delaySeconds=5)

    maybeKafkaDF match {
      case Some(kafkaStreamDF) =>
          val messages = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          
          val parsedMessages = messages.select(
            from_json(col("value"), REVIEW_SCHEMA).as("data")
          ).select("data.*")
          
          parsedMessages.writeStream
            .foreachBatch { (newBatch: DataFrame, batchId: Long) =>

              val allReviews = updateReviewTable(spark, newBatch)

              val activeBusinessIds = allReviews.select("business_id").distinct()
              val activeUserIds = allReviews.select("user_id").distinct()
              val filteredBusiness = businessDF.join(activeBusinessIds, "business_id")
              val filteredUsers = usersDF.join(activeUserIds, "user_id")

              val allUsers = updateUserTable(filteredUsers)
              val allBusiness = updateBusinessTable(filteredBusiness)



              //println(s"allReviews ${allReviews.count()} allUsers ${allUsers.count()} allBusiness ${allBusiness.count()}")
              println(s"allReviews ${allReviews.count()} filteredUsers ${filteredUsers.count()} filteredBusiness ${filteredBusiness.count()}")

              processAllBusinessAnalytics(spark, filteredBusiness, filteredUsers, allReviews)
              
              println(s"Batch $batchId traité et statistiques insérées.")
            }
            .outputMode("append")
            .start()
            .awaitTermination()

      case None =>
        println("Le topic Kafka n’est pas disponible. Fermeture de l'application.")
        System.exit(1)
    }
  }

  @tailrec
  private def tryConnect(spark: SparkSession, attempt: Int, retries: Int, delaySeconds: Int): Option[DataFrame] = {
      println(s"Tentative $attempt/$retries de connexion au topic Kafka...")
      Try {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
          .option("subscribe", REVIEW_TOPIC)
          .option("startingOffsets", "earliest")
          .load()
      } match {
        // Connection reussi
        case Success(df) =>
          println(s"Connexion établie avec le topic Kafka '$REVIEW_TOPIC'")
          Some(df)

        // Échec Connection Nouvelle tentative
        case Failure(e) if attempt < retries =>
          println(s"Échec tentative $attempt : ${e.getMessage}")
          Thread.sleep(delaySeconds * 1000)
          tryConnect(spark, attempt + 1, retries, delaySeconds)
        
        // Échec Connection arret de l'application
        case Failure(e) =>
          println(s"Échec après $retries tentatives. Abandon.")
          None
      }
  }
}
