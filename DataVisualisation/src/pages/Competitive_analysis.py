import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from typing import Dict, List, Tuple, Optional
import json
import psycopg2
import os
from sqlalchemy import create_engine

# ================== CONFIGURATION DATABASE ==================
DB_CONFIG = {
    'host': os.getenv("DATABASE_HOST", "localhost"),
    'port': os.getenv("DATABASE_PORT", 5432),
    'dbname': os.getenv("DATABASE_NAME", "spark_streaming_db"),
    'user': os.getenv("DATABASE_USER", "divinandretomadam"),
    'password': os.getenv("DATABASE_PASSWORD", "oDAnmvidrTnmeiAa")
}

def get_db_connection():
    """Créer une connexion à la base de données PostgreSQL"""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None

# ================== COUCHE DE DONNÉES ==================
class YelpCompetitiveDatabaseData:
    def __init__(self):
        self.engine = get_db_connection()
        self.business_profiles = None
        self.competitor_mappings = None
        self.competitive_analysis = None
        self.market_positioning = None
        self.detailed_comparisons = None
        self.market_shares = None
        self.competitive_insights = None
        
    def load_all_data(self):
        """Charge toutes les données depuis la base PostgreSQL"""
        if self.engine is None:
            st.error("Impossible de se connecter à la base de données")
            return False
            
        try:
            # Chargement de toutes les tables d'analyse concurrentielle
            self.business_profiles = pd.read_sql("SELECT * FROM business_profiles", self.engine)
            self.competitor_mappings = pd.read_sql("SELECT * FROM competitor_mappings", self.engine)
            self.competitive_analysis = pd.read_sql("SELECT * FROM competitive_analysis", self.engine)
            self.market_positioning = pd.read_sql("SELECT * FROM market_positioning", self.engine)
            self.detailed_comparisons = pd.read_sql("SELECT * FROM detailed_comparisons", self.engine)
            self.market_shares = pd.read_sql("SELECT * FROM market_shares", self.engine)
            self.competitive_insights = pd.read_sql("SELECT * FROM competitive_insights", self.engine)
            
            return True
        except Exception as e:
            st.error(f"Erreur lors du chargement des données: {e}")
            return False
    
    def get_business_list(self) -> List[Dict]:
        """Retourne la liste des entreprises disponibles"""
        if self.business_profiles is None or self.business_profiles.empty:
            return []
        
        businesses = self.business_profiles[['business_id', 'name', 'city', 'state', 'categories']].copy()
        businesses['display_name'] = businesses['name'] + ' (' + businesses['city'] + ', ' + businesses['state'] + ')'
        
        return businesses.to_dict('records')

# ================== COUCHE D'ANALYSE CONCURRENTIELLE ==================
class CompetitiveDatabaseAnalyzer:
    def __init__(self, data: YelpCompetitiveDatabaseData):
        self.data = data
        
    def get_target_business_info(self, business_id: str) -> Dict:
        """Informations de l'entreprise cible depuis la base"""
        if self.data.business_profiles is None:
            return {}
            
        business_data = self.data.business_profiles[
            self.data.business_profiles['business_id'] == business_id
        ]
        
        if business_data.empty:
            return {'message': 'Entreprise non trouvée'}
            
        business = business_data.iloc[0]
        
        return {
            'name': business['name'],
            'city': business.get('city', ''),
            'state': business.get('state', ''),
            'categories': business.get('categories', ''),
            'total_reviews': int(business.get('total_reviews', 0)),
            'average_rating': float(business.get('average_rating', 0)),
            'recent_average': float(business.get('recent_average', 0)),
            'rating_stddev': float(business.get('rating_stddev', 0)),
            'last_review_date': business.get('last_review_date', 'N/A')
        }
    
    def find_competitors(self, business_id: str, max_competitors: int = 20) -> List[Dict]:
        """Trouve les concurrents directs depuis la base"""
        if self.data.detailed_comparisons is None:
            return []
            
        competitors_data = self.data.detailed_comparisons[
            self.data.detailed_comparisons['target_business_id'] == business_id
        ].head(max_competitors)
        
        if competitors_data.empty:
            return []
        
        competitors = []
        for _, comp in competitors_data.iterrows():
            # Récupérer la distribution des notes depuis les colonnes
            rating_distribution = {
                1: int(comp.get('competitor_rating_1', 0)),
                2: int(comp.get('competitor_rating_2', 0)),
                3: int(comp.get('competitor_rating_3', 0)),
                4: int(comp.get('competitor_rating_4', 0)),
                5: int(comp.get('competitor_rating_5', 0))
            }
            
            competitors.append({
                'business_id': comp['competitor_business_id'],
                'name': comp['competitor_name'],
                'city': comp.get('competitor_city', ''),
                'state': comp.get('competitor_state', ''),
                'avg_rating': float(comp.get('competitor_average_rating', 0)),
                'recent_avg': float(comp.get('competitor_recent_average', 0)),
                'review_count': int(comp.get('competitor_total_reviews', 0)),
                'rating_distribution': rating_distribution,
                'is_same_city': bool(comp.get('is_same_city', False)),
                'rank': int(comp.get('competitor_rank', 0))
            })
        
        return competitors
    
    def get_market_positioning(self, business_id: str) -> Dict:
        """Analyse du positionnement sur le marché depuis la base"""
        if self.data.market_positioning is None:
            return {
                'position': 'Non déterminé',
                'percentile_rating': 0,
                'percentile_popularity': 0,
                'strengths': [],
                'weaknesses': []
            }
        
        positioning_data = self.data.market_positioning[
            self.data.market_positioning['target_business_id'] == business_id
        ]
        
        if positioning_data.empty:
            return {
                'position': 'Non déterminé',
                'percentile_rating': 0,
                'percentile_popularity': 0,
                'strengths': [],
                'weaknesses': []
            }
        
        positioning = positioning_data.iloc[0]
        
        # Parser les forces et faiblesses (stockées comme chaînes séparées par des virgules)
        strengths = [s.strip() for s in str(positioning.get('strengths', '')).split(',') if s.strip()]
        weaknesses = [w.strip() for w in str(positioning.get('weaknesses', '')).split(',') if w.strip()]
        
        return {
            'position': positioning.get('market_position', 'Non déterminé'),
            'percentile_rating': float(positioning.get('rating_percentile', 0)),
            'percentile_popularity': float(positioning.get('popularity_percentile', 0)),
            'total_competitors': int(positioning.get('total_competitors', 0)),
            'same_city_competitors': int(positioning.get('same_city_competitors', 0)),
            'best_competitor_rating': float(positioning.get('best_competitor_rating', 0)),
            'avg_competitor_rating': float(positioning.get('avg_competitor_rating', 0)),
            'avg_competitor_reviews': float(positioning.get('avg_competitor_reviews', 0)),
            'strengths': strengths,
            'weaknesses': weaknesses
        }
    
    def get_detailed_comparison(self, business_id: str, competitors: List[Dict]) -> Dict:
        """Comparaison détaillée avec les top concurrents"""
        target_info = self.get_target_business_info(business_id)
        
        if not competitors:
            return {'comparison_data': [], 'top_competitor': None}
        
        comparison_data = []
        
        # Ajouter l'entreprise cible (obtenir sa distribution depuis business_profiles)
        if self.data.business_profiles is not None:
            target_profile = self.data.business_profiles[
                self.data.business_profiles['business_id'] == business_id
            ]
            
            if not target_profile.empty:
                target = target_profile.iloc[0]
                comparison_data.append({
                    'name': target_info['name'] + ' (Vous)',
                    'avg_rating': target_info['average_rating'],
                    'review_count': target_info['total_reviews'],
                    'rating_1': int(target.get('rating_1', 0)),
                    'rating_2': int(target.get('rating_2', 0)),
                    'rating_3': int(target.get('rating_3', 0)),
                    'rating_4': int(target.get('rating_4', 0)),
                    'rating_5': int(target.get('rating_5', 0)),
                    'is_target': True
                })
        
        # Ajouter les top 5 concurrents
        top_competitors = sorted(competitors, key=lambda x: x['avg_rating'], reverse=True)[:5]
        
        for comp in top_competitors:
            comparison_data.append({
                'name': comp['name'],
                'avg_rating': comp['avg_rating'],
                'review_count': comp['review_count'],
                'rating_1': comp['rating_distribution'].get(1, 0),
                'rating_2': comp['rating_distribution'].get(2, 0),
                'rating_3': comp['rating_distribution'].get(3, 0),
                'rating_4': comp['rating_distribution'].get(4, 0),
                'rating_5': comp['rating_distribution'].get(5, 0),
                'is_target': False
            })
        
        return {
            'comparison_data': comparison_data,
            'top_competitor': top_competitors[0] if top_competitors else None
        }
    
    def get_competitive_insights(self, business_id: str) -> Dict:
        """Récupère les insights concurrentiels depuis la base"""
        if self.data.competitive_insights is None:
            return {}
        
        insights_data = self.data.competitive_insights[
            self.data.competitive_insights['target_business_id'] == business_id
        ]
        
        if insights_data.empty:
            return {}
        
        insights = insights_data.iloc[0]
        
        return {
            'primary_insight': insights.get('primary_insight', ''),
            'recommended_action': insights.get('recommended_action', ''),
            'competitive_advantage': insights.get('competitive_advantage', '')
        }
    
    def get_market_share(self, business_id: str) -> Dict:
        """Récupère les données de part de marché"""
        if self.data.market_shares is None:
            return {'market_share_data': [], 'total_market_reviews': 0}
        
        # Obtenir les infos de l'entreprise cible
        target_info = self.get_target_business_info(business_id)
        
        if not target_info:
            return {'market_share_data': [], 'total_market_reviews': 0}
        
        # Filtrer les données de part de marché pour la même zone et catégorie
        market_data = self.data.market_shares[
            (self.data.market_shares['target_city'] == target_info['city']) &
            (self.data.market_shares['target_state'] == target_info['state'])
        ]
        
        if market_data.empty:
            return {'market_share_data': [], 'total_market_reviews': 0}
        
        # Prendre les top 10 entreprises du même marché
        top_market = market_data.head(10)
        
        return {
            'market_share_data': top_market.to_dict('records'),
            'total_market_reviews': int(market_data['total_market_reviews'].iloc[0]) if len(market_data) > 0 else 0
        }

# ================== COUCHE DE VISUALISATION ==================
class CompetitiveDatabaseVisualizer:
    def __init__(self, analyzer: CompetitiveDatabaseAnalyzer):
        self.analyzer = analyzer
    
    def create_market_overview_chart(self, business_id: str, competitors: List[Dict]):
        """Graphique de vue d'ensemble du marché"""
        if not competitors:
            return None
        
        target_info = self.analyzer.get_target_business_info(business_id)
        
        # Préparer les données
        all_data = competitors.copy()
        all_data.append({
            'name': target_info['name'] + ' (Vous)',
            'avg_rating': target_info['average_rating'],
            'review_count': target_info['total_reviews'],
            'is_target': True
        })
        
        # Ajouter la colonne is_target pour les concurrents
        for comp in competitors:
            comp['is_target'] = False
        
        df = pd.DataFrame(all_data)
        
        # Graphique en bulles
        fig = px.scatter(
            df,
            x='avg_rating',
            y='review_count',
            size='review_count',
            color='is_target',
            hover_name='name',
            hover_data=['avg_rating', 'review_count'],
            title='Positionnement concurrentiel - Note vs Popularité',
            labels={
                'avg_rating': 'Note moyenne (étoiles)',
                'review_count': 'Nombre d\'avis (popularité)',
                'is_target': 'Type'
            },
            color_discrete_map={True: '#FF6B6B', False: '#4ECDC4'}
        )
        
        fig.update_layout(
            xaxis=dict(range=[0, 5.5]),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Ajouter des lignes de référence
        if len(df) > 1:
            avg_rating_all = df['avg_rating'].mean()
            avg_reviews_all = df['review_count'].mean()
            
            fig.add_hline(y=avg_reviews_all, line_dash="dash", line_color="gray", opacity=0.5)
            fig.add_vline(x=avg_rating_all, line_dash="dash", line_color="gray", opacity=0.5)
        
        return fig
    
    def create_rating_comparison_chart(self, comparison_data: List[Dict]):
        """Graphique de comparaison des notes"""
        if not comparison_data:
            return None
        
        df = pd.DataFrame(comparison_data)
        
        # Graphique en barres horizontales
        fig = px.bar(
            df,
            x='avg_rating',
            y='name',
            orientation='h',
            color='is_target',
            title='Comparaison des notes moyennes',
            labels={'avg_rating': 'Note moyenne', 'name': 'Entreprise'},
            color_discrete_map={True: '#FF6B6B', False: '#4ECDC4'}
        )
        
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            xaxis=dict(range=[0, 5])
        )
        
        return fig
    
    def create_rating_distribution_comparison(self, comparison_data: List[Dict]):
        """Graphique de distribution des notes en comparaison"""
        if not comparison_data:
            return None
        
        df = pd.DataFrame(comparison_data)
        
        # Transformer les données pour le graphique empilé
        rating_cols = ['rating_1', 'rating_2', 'rating_3', 'rating_4', 'rating_5']
        
        fig = go.Figure()
        
        colors = ['#FF4444', '#FF8800', '#FFBB00', '#88CC00', '#00AA44']
        
        for i, col in enumerate(rating_cols):
            fig.add_trace(go.Bar(
                name=f'{i+1} étoile{"s" if i > 0 else ""}',
                x=df['name'],
                y=df[col],
                marker_color=colors[i]
            ))
        
        fig.update_layout(
            title='Distribution des notes par concurrent',
            xaxis_title='Entreprise',
            yaxis_title='Nombre d\'avis',
            barmode='stack',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
    
    def create_market_share_chart(self, market_share_data: Dict):
        """Graphique de parts de marché"""
        if not market_share_data.get('market_share_data'):
            return None
        
        df = pd.DataFrame(market_share_data['market_share_data'])
        
        # Limiter les noms trop longs
        df['short_name'] = df['target_name'].apply(
            lambda x: x[:20] + '...' if len(str(x)) > 20 else str(x)
        )
        
        # Graphique en secteurs
        fig = px.pie(
            df.head(10),  # Top 10
            values='market_share_pct',
            names='short_name',
            title='Parts de marché (basé sur le volume d\'avis)'
        )
        
        return fig

# ================== INTERFACE STREAMLIT ==================
def main():
    st.set_page_config(
        page_title="Analyse Concurrentielle Yelp - Database",
        page_icon="🏆",
        layout="wide"
    )
    
    st.title("🏆 Analyse Concurrentielle Yelp (Database)")
    st.markdown("Découvrez vos concurrents directs et positionnez-vous sur le marché depuis PostgreSQL")
    st.markdown("---")
    
    # Initialisation des données
    if 'competitive_db_data' not in st.session_state:
        st.session_state.competitive_db_data = YelpCompetitiveDatabaseData()
        st.session_state.competitive_db_loaded = False
    
    # Chargement des données depuis la base
    if not st.session_state.competitive_db_loaded:
        with st.spinner("Chargement des données d'analyse concurrentielle depuis PostgreSQL..."):
            if st.session_state.competitive_db_data.load_all_data():
                st.session_state.competitive_db_loaded = True
                st.success("Données d'analyse concurrentielle chargées avec succès!")
            else:
                st.error("Impossible de charger les données d'analyse concurrentielle")
                st.stop()
    
    # Sidebar pour informations et contrôles
    with st.sidebar:
        st.header("🎯 Configuration")
        
        if st.session_state.competitive_db_loaded:
            st.success("✅ Connexion PostgreSQL active")
            
            data = st.session_state.competitive_db_data
            
            if data.business_profiles is not None:
                st.info(f"📈 {len(data.business_profiles)} entreprises")
            if data.competitor_mappings is not None:
                st.info(f"🔗 {len(data.competitor_mappings)} relations concurrentielles")
            if data.competitive_insights is not None:
                st.info(f"💡 {len(data.competitive_insights)} insights")
        
        # Paramètres d'analyse
        max_competitors = st.slider(
            "Nombre max de concurrents",
            min_value=5,
            max_value=50,
            value=20,
            step=5,
            help="Plus le nombre est élevé, plus l'analyse sera complète"
        )
        
        st.markdown("---")
        st.subheader("📊 À propos")
        st.write("Cette page identifie vos concurrents directs basés sur :")
        st.write("• 📍 **Localisation** (même ville/état)")
        st.write("• 🏷️ **Catégorie d'activité**")
        st.write("• 📈 **Volume d'activité**")
        
        st.markdown("---")
        st.text("🔗 Base: PostgreSQL")
        st.text(f"🏠 Host: {DB_CONFIG['host']}")
        st.text(f"📋 DB: {DB_CONFIG['dbname']}")
        
        if st.button("🔄 Recharger depuis la base"):
            st.session_state.competitive_db_loaded = False
            st.rerun()
    
    # Interface principale
    if st.session_state.competitive_db_loaded:
        analyzer = CompetitiveDatabaseAnalyzer(st.session_state.competitive_db_data)
        visualizer = CompetitiveDatabaseVisualizer(analyzer)
        
        # Sélection de l'entreprise
        st.header("🏢 Sélection de l'entreprise à analyser")
        businesses = st.session_state.competitive_db_data.get_business_list()
        
        if businesses:
            business_names = [b['display_name'] for b in businesses]
            selected_business = st.selectbox(
                "Choisissez votre entreprise:",
                business_names,
                key="business_selector"
            )
            
            # Trouver l'ID de l'entreprise sélectionnée
            selected_id = next(b['business_id'] for b in businesses if b['display_name'] == selected_business)
            
            # Recherche des concurrents depuis la base
            with st.spinner("Chargement des concurrents depuis la base..."):
                competitors = analyzer.find_competitors(selected_id, max_competitors)
            
            if competitors:
                st.success(f"✅ {len(competitors)} concurrents trouvés")
                
                # Informations de base
                target_info = analyzer.get_target_business_info(selected_id)
                st.markdown("---")
                
                # Vue d'ensemble concise
                st.header("📊 Entreprise sélectionnée")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Note moyenne", f"{target_info['average_rating']:.2f}/5")
                
                with col2:
                    st.metric("Total avis", f"{target_info['total_reviews']:,}")
                
                with col3:
                    st.metric("Ville", target_info['city'])
                
                with col4:
                    st.metric("Concurrents trouvés", len(competitors))
                
                st.info(f"**Catégories:** {target_info['categories']}")
                if target_info.get('last_review_date'):
                    st.info(f"**Dernier avis:** {target_info['last_review_date']}")
                
                # Positionnement sur le marché
                positioning = analyzer.get_market_positioning(selected_id)
                
                st.markdown("---")
                st.header("📈 Positionnement sur le marché")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Position", positioning['position'])
                
                with col2:
                    st.metric("Percentile notes", f"{positioning['percentile_rating']:.0f}%")
                
                with col3:
                    st.metric("Percentile popularité", f"{positioning['percentile_popularity']:.0f}%")
                
                with col4:
                    st.metric("Concurrents totaux", positioning['total_competitors'])
                
                # Table des concurrents
                st.markdown("---")
                st.header("🏆 Liste des concurrents directs")
                
                # Préparer les données pour l'affichage
                competitors_df = pd.DataFrame(competitors)[
                    ['name', 'city', 'avg_rating', 'recent_avg', 'review_count', 'is_same_city']
                ]
                competitors_df.columns = [
                    'Nom', 'Ville', 'Note moyenne', 'Tendance récente', 'Nb avis', 'Même ville'
                ]
                competitors_df['Même ville'] = competitors_df['Même ville'].map({True: '✅', False: '❌'})
                
                st.dataframe(
                    competitors_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Statistiques sur les concurrents
                st.markdown("---")
                st.header("📈 Aperçu statistique des concurrents")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    avg_competitor_rating = positioning.get('avg_competitor_rating', 0)
                    st.metric("Note moyenne concurrents", f"{avg_competitor_rating:.2f}/5")
                
                with col2:
                    best_competitor = positioning.get('best_competitor_rating', 0)
                    st.metric("Meilleur concurrent", f"{best_competitor:.2f}/5")
                
                with col3:
                    same_city_count = positioning.get('same_city_competitors', 0)
                    st.metric("Concurrents même ville", same_city_count)
                
                with col4:
                    avg_competitor_reviews = positioning.get('avg_competitor_reviews', 0)
                    st.metric("Avis moyens concurrent", f"{avg_competitor_reviews:.0f}")
                
                # Graphiques d'analyse
                st.markdown("---")
                st.header("📊 Analyses visuelles")
                
                tab1, tab2, tab3, tab4 = st.tabs([
                    "Positionnement marché", 
                    "Comparaison notes", 
                    "Distribution détaillée",
                    "Parts de marché"
                ])
                
                with tab1:
                    chart = visualizer.create_market_overview_chart(selected_id, competitors)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.warning("Impossible de créer le graphique de positionnement")
                
                with tab2:
                    comparison = analyzer.get_detailed_comparison(selected_id, competitors)
                    chart = visualizer.create_rating_comparison_chart(comparison['comparison_data'])
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.warning("Impossible de créer le graphique de comparaison")
                
                with tab3:
                    comparison = analyzer.get_detailed_comparison(selected_id, competitors)
                    chart = visualizer.create_rating_distribution_comparison(comparison['comparison_data'])
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.warning("Impossible de créer le graphique de distribution")
                
                with tab4:
                    market_share = analyzer.get_market_share(selected_id)
                    chart = visualizer.create_market_share_chart(market_share)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.warning("Aucune donnée de part de marché disponible")
                
                # Insights et recommandations
                st.markdown("---")
                st.header("💡 Insights et recommandations")
                
                insights = analyzer.get_competitive_insights(selected_id)
                
                if insights:
                    if insights.get('primary_insight'):
                        st.info(f"🎯 **Insight principal :** {insights['primary_insight']}")
                    
                    if insights.get('competitive_advantage'):
                        st.success(f"🏆 **Avantage concurrentiel :** {insights['competitive_advantage']}")
                    
                    if insights.get('recommended_action'):
                        st.warning(f"🚀 **Action recommandée :** {insights['recommended_action']}")
                
                # Position globale et actions
                if positioning['percentile_rating'] >= 70:
                    st.success(f"🎯 **Excellente position** - Vous êtes dans le top 30% des notes ({positioning['percentile_rating']:.0f}e percentile)")
                elif positioning['percentile_rating'] >= 50:
                    st.info(f"📊 **Position correcte** - Notes dans la moyenne supérieure ({positioning['percentile_rating']:.0f}e percentile)")
                else:
                    st.warning(f"⚠️ **Marge d'amélioration** - Notes en dessous de la moyenne ({positioning['percentile_rating']:.0f}e percentile)")
                
                # Popularité
                if positioning['percentile_popularity'] >= 70:
                    st.success(f"📢 **Forte visibilité** - Plus populaire que {positioning['percentile_popularity']:.0f}% des concurrents")
                elif positioning['percentile_popularity'] >= 50:
                    st.info(f"👥 **Visibilité correcte** - Popularité dans la moyenne ({positioning['percentile_popularity']:.0f}e percentile)")
                else:
                    st.warning(f"📈 **Besoin de visibilité** - Moins d'avis que {100-positioning['percentile_popularity']:.0f}% des concurrents")
                
                # Forces et faiblesses
                if positioning.get('strengths'):
                    st.subheader("💪 Forces identifiées")
                    for strength in positioning['strengths']:
                        if strength:  # Éviter les chaînes vides
                            st.write(f"• ✅ {strength}")
                
                if positioning.get('weaknesses'):
                    st.subheader("⚠️ Points d'amélioration")
                    for weakness in positioning['weaknesses']:
                        if weakness:  # Éviter les chaînes vides
                            st.write(f"• 🔧 {weakness}")
                
                # Actions recommandées détaillées
                st.subheader("🚀 Plan d'action recommandé")
                
                actions = []
                
                if positioning['percentile_rating'] < 50:
                    actions.append("🎯 **Améliorer la qualité** : Se concentrer sur l'expérience client")
                    actions.append("📝 **Analyser les avis négatifs** : Identifier les points de friction")
                
                if positioning['percentile_popularity'] < 50:
                    actions.append("📢 **Augmenter la visibilité** : Encourager les clients satisfaits à laisser des avis")
                    actions.append("🔍 **Marketing local** : Renforcer la présence dans la zone géographique")
                
                if positioning.get('same_city_competitors', 0) < 3:
                    actions.append("🌍 **Exploiter l'avantage géographique** : Peu de concurrence directe dans votre ville")
                
                if positioning.get('best_competitor_rating', 0) > target_info['average_rating'] + 0.5:
                    actions.append("📈 **Rattraper le leader** : Analyser les meilleures pratiques du concurrent le mieux noté")
                
                if not actions:
                    actions.append("✅ **Maintenir l'excellence** : Continuer sur la voie actuelle et surveiller la concurrence")
                
                for action in actions:
                    st.write(f"• {action}")
                
                # Résumé exécutif
                st.markdown("---")
                st.header("📋 Résumé exécutif")
                
                summary_col1, summary_col2 = st.columns(2)
                
                with summary_col1:
                    st.subheader("🎯 Position actuelle")
                    st.write(f"**Classement :** {positioning['position']}")
                    st.write(f"**Performance notes :** {positioning['percentile_rating']:.0f}e percentile")
                    st.write(f"**Performance popularité :** {positioning['percentile_popularity']:.0f}e percentile")
                    st.write(f"**Concurrence locale :** {positioning.get('same_city_competitors', 0)} entreprises")
                
                with summary_col2:
                    st.subheader("📊 Contexte concurrentiel")
                    st.write(f"**Total concurrents :** {positioning['total_competitors']}")
                    st.write(f"**Note moyenne marché :** {positioning.get('avg_competitor_rating', 0):.2f}/5")
                    st.write(f"**Meilleur concurrent :** {positioning.get('best_competitor_rating', 0):.2f}/5")
                    
                    # Écart avec le leader
                    if positioning.get('best_competitor_rating', 0) > 0:
                        gap = positioning['best_competitor_rating'] - target_info['average_rating']
                        if gap > 0:
                            st.write(f"**Écart avec le leader :** -{gap:.2f} étoiles")
                        else:
                            st.write(f"**Avance sur les concurrents :** +{abs(gap):.2f} étoiles")
            
            else:
                st.warning("😔 Aucun concurrent trouvé pour cette entreprise")
                st.info("Cela peut être dû à:")
                st.write("• Catégorie d'activité très spécifique")
                st.write("• Localisation isolée") 
                st.write("• Données insuffisantes dans le dataset")
                
                # Suggestions alternatives
                st.subheader("💡 Suggestions")
                st.write("• Essayez une autre entreprise avec plus d'activité")
                st.write("• Vérifiez que l'entreprise a une catégorie définie")
                st.write("• Contactez l'administrateur si le problème persiste")
        
        else:
            st.warning("Aucune entreprise trouvée dans la base de données")
            st.info("Assurez-vous que:")
            st.write("• Les données ont été chargées correctement")
            st.write("• Le pipeline Spark a traité les analyses concurrentielles")
            st.write("• La table business_profiles contient des données")
    
    else:
        st.info("🔄 Chargement des données d'analyse concurrentielle en cours...")

if __name__ == "__main__":
    main()