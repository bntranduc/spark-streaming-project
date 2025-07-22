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
import matplotlib.pyplot as plt
import re
from wordcloud import WordCloud, STOPWORDS

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

# ================== COUCHE DE DONNÉES (LECTURE SEULE) ==================
class YelpMarketData:
    """Classe pour lire les données d'analyse de marché traitées par Spark"""
    
    def __init__(self):
        self.engine = get_db_connection()
        
        # Tables d'analyse de marché créées par Spark
        self.market_locations = None
        self.market_categories = None
        self.market_overview = None
        self.market_segments = None
        self.market_quarterly_trends = None
        self.market_trends_summary = None
        self.market_opportunities = None
        self.market_rating_distribution = None
        
        # Tables d'analyse business existantes
        self.business_overview = None
        self.rating_distribution = None
        self.temporal_analysis = None
        self.trend_analysis = None
        self.business_user_stats = None
        self.top_reviewers = None
        
    def load_all_data(self):
        """Charge toutes les données depuis PostgreSQL (tables créées par Spark)"""
        if self.engine is None:
            st.error("Impossible de se connecter à la base de données")
            return False
            
        try:
            # Tables d'analyse de marché (créées par Spark Scala)
            self.market_locations = pd.read_sql("SELECT * FROM market_locations ORDER BY business_count DESC", self.engine)
            self.market_categories = pd.read_sql("SELECT * FROM market_categories ORDER BY opportunity_score DESC", self.engine)
            self.market_overview = pd.read_sql("SELECT * FROM market_overview ORDER BY total_businesses DESC", self.engine)
            self.market_segments = pd.read_sql("SELECT * FROM market_segments ORDER BY opportunity_score DESC", self.engine)
            self.market_quarterly_trends = pd.read_sql("SELECT * FROM market_quarterly_trends ORDER BY location_key, quarter", self.engine)
            self.market_trends_summary = pd.read_sql("SELECT * FROM market_trends_summary ORDER BY rating_trend DESC", self.engine)
            self.market_opportunities = pd.read_sql("SELECT * FROM market_opportunities ORDER BY opportunity_score DESC", self.engine)
            self.market_rating_distribution = pd.read_sql("SELECT * FROM market_rating_distribution ORDER BY location_key, rating", self.engine)
            
            # Tables d'analyse business existantes
            self.business_overview = pd.read_sql("SELECT * FROM business_overview ORDER BY total_reviews DESC", self.engine)
            self.rating_distribution = pd.read_sql("SELECT * FROM rating_distribution", self.engine)
            self.temporal_analysis = pd.read_sql("SELECT * FROM temporal_analysis", self.engine)
            self.trend_analysis = pd.read_sql("SELECT * FROM trend_analysis", self.engine)
            self.business_user_stats = pd.read_sql("SELECT * FROM business_user_stats", self.engine)
            self.top_reviewers = pd.read_sql("SELECT * FROM top_reviewers", self.engine)
            
            return True
        except Exception as e:
            st.error(f"Erreur lors du chargement des données: {e}")
            st.error("Assurez-vous que le traitement Spark Scala a été exécuté et que toutes les tables sont créées.")
            return False
    
    def get_market_locations_list(self) -> List[Dict]:
        """Retourne la liste des localisations de marché"""
        if self.market_locations is None or self.market_locations.empty:
            return []
        return self.market_locations.to_dict('records')
    
    def get_business_list(self) -> List[Dict]:
        """Retourne la liste des entreprises"""
        if self.business_overview is None or self.business_overview.empty:
            return []
            
        businesses = self.business_overview[['business_id', 'name', 'city', 'state', 'categories']].copy()
        businesses['display_name'] = businesses['name'] + ' (' + businesses['city'] + ', ' + businesses['state'] + ')'
        
        return businesses.to_dict('records')

# ================== COUCHE DE VISUALISATION ==================
class YelpMarketVisualizer:
    """Visualisations pour l'analyse de marché"""
    
    def __init__(self, data: YelpMarketData):
        self.data = data
    
    def create_market_locations_chart(self):
        """Graphique des meilleures localisations de marché"""
        if self.data.market_locations is None or self.data.market_locations.empty:
            return None
            
        df = self.data.market_locations.head(15)
        
        fig = px.bar(
            df,
            x='display_name',
            y='business_count',
            color='avg_rating',
            hover_data=['total_reviews', 'unique_reviewers'],
            title="Top 15 des localisations par nombre d'entreprises",
            labels={'display_name': 'Localisation', 'business_count': 'Nombre d\'entreprises'},
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=600)
        
        return fig
    
    def create_market_categories_chart(self):
        """Graphique des catégories d'opportunités"""
        if self.data.market_categories is None or self.data.market_categories.empty:
            return None
            
        df = self.data.market_categories.head(15)
        
        fig = px.scatter(
            df,
            x='business_count',
            y='avg_rating',
            size='total_reviews',
            color='opportunity_score',
            hover_name='category',
            hover_data=['saturation'],
            title="Analyse des catégories de marché",
            labels={'business_count': 'Nombre d\'entreprises', 'avg_rating': 'Note moyenne'},
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_layout(height=600)
        
        return fig
    
    def create_market_overview_chart(self, selected_location: str = None):
        """Vue d'ensemble d'un marché spécifique"""
        if self.data.market_overview is None or self.data.market_overview.empty:
            return None
        
        if selected_location:
            df = self.data.market_overview[
                self.data.market_overview['location_key'] == selected_location
            ]
        else:
            df = self.data.market_overview.head(10)
        
        if df.empty:
            return None
            
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Total des entreprises',
            x=df['location_key'],
            y=df['total_businesses'],
            yaxis='y'
        ))
        
        fig.add_trace(go.Scatter(
            name='Note moyenne du marché',
            x=df['location_key'],
            y=df['avg_market_rating'],
            yaxis='y2',
            mode='lines+markers',
            line=dict(color='red')
        ))
        
        fig.update_layout(
            title='Vue d\'ensemble des marchés',
            xaxis=dict(title='Localisation'),
            yaxis=dict(title='Nombre d\'entreprises', side='left'),
            yaxis2=dict(title='Note moyenne', side='right', overlaying='y'),
            height=500
        )
        
        return fig
    
    def create_market_trends_chart(self, selected_location: str = None):
        """Graphique des tendances temporelles"""
        if self.data.market_quarterly_trends is None or self.data.market_quarterly_trends.empty:
            return None
        
        if selected_location:
            df = self.data.market_quarterly_trends[
                self.data.market_quarterly_trends['location_key'] == selected_location
            ].sort_values('quarter')
        else:
            # Prendre les 5 localisations les plus actives
            top_locations = self.data.market_quarterly_trends.groupby('location_key')['review_count'].sum().nlargest(5).index
            df = self.data.market_quarterly_trends[
                self.data.market_quarterly_trends['location_key'].isin(top_locations)
            ].sort_values(['location_key', 'quarter'])
        
        if df.empty:
            return None
        
        fig = px.line(
            df,
            x='quarter',
            y='avg_rating',
            color='location_key',
            title='Évolution de la qualité par marché',
            labels={'quarter': 'Trimestre', 'avg_rating': 'Note moyenne'},
            markers=True
        )
        
        fig.update_layout(height=500)
        
        return fig
    
    def create_opportunities_chart(self):
        """Graphique des opportunités de marché"""
        if self.data.market_opportunities is None or self.data.market_opportunities.empty:
            return None
            
        df = self.data.market_opportunities.head(20)
        
        fig = px.scatter(
            df,
            x='business_count',
            y='avg_rating',
            size='total_reviews',
            color='opportunity_score',
            hover_name='category',
            facet_col='state',
            facet_col_wrap=3,
            title="Opportunités de marché par état et catégorie",
            labels={'business_count': 'Concurrence', 'avg_rating': 'Qualité actuelle'},
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_layout(height=800)
        
        return fig
    
    def create_rating_distribution_market_chart(self, selected_location: str):
        """Distribution des notes pour un marché spécifique"""
        if self.data.market_rating_distribution is None or self.data.market_rating_distribution.empty:
            return None
        
        df = self.data.market_rating_distribution[
            self.data.market_rating_distribution['location_key'] == selected_location
        ]
        
        if df.empty:
            return None
        
        fig = px.bar(
            df,
            x='rating',
            y='review_count',
            title=f"Distribution des notes - {selected_location}",
            labels={'rating': 'Note (étoiles)', 'review_count': 'Nombre d\'avis'},
            color='rating',
            color_continuous_scale='RdYlGn'
        )
        
        return fig

# ================== INTERFACE STREAMLIT ==================
def main():
    st.set_page_config(
        page_title="Yelp Market Analytics Dashboard",
        page_icon="🏪",
        layout="wide"
    )
    
    st.title("🏪 Yelp Market Analytics Dashboard")
    st.markdown("### Analyse de marché basée sur les données traitées par Spark Scala")
    st.markdown("---")
    
    # Initialisation des données
    if 'yelp_market_data' not in st.session_state:
        st.session_state.yelp_market_data = YelpMarketData()
        st.session_state.market_data_loaded = False
    
    # Chargement des données
    if not st.session_state.market_data_loaded:
        with st.spinner("Chargement des données d'analyse de marché depuis PostgreSQL..."):
            if st.session_state.yelp_market_data.load_all_data():
                st.session_state.market_data_loaded = True
                st.success("Données d'analyse de marché chargées avec succès!")
            else:
                st.error("Impossible de charger les données d'analyse de marché")
                st.error("Veuillez exécuter le traitement Spark Scala (MarketAnalysis) avant d'utiliser ce dashboard.")
                st.stop()
    
    # Sidebar
    with st.sidebar:
        st.header("🏪 Analyse de Marché")
        
        if st.session_state.market_data_loaded:
            st.success("✅ Données Spark chargées")
            
            data = st.session_state.yelp_market_data
            
            # Statistiques générales
            if data.market_locations is not None:
                st.info(f"📍 {len(data.market_locations)} localisations analysées")
            if data.market_categories is not None:
                st.info(f"🏷️ {len(data.market_categories)} catégories")
            if data.market_opportunities is not None:
                st.info(f"💡 {len(data.market_opportunities)} opportunités détectées")
        
        st.markdown("---")
        st.text("🔗 Source: Spark Scala + PostgreSQL")
        st.text(f"🏠 Host: {DB_CONFIG['host']}")
        st.text(f"📋 DB: {DB_CONFIG['dbname']}")
        
        if st.button("🔄 Recharger les données"):
            st.session_state.market_data_loaded = False
            st.rerun()
    
    # Interface principale
    if st.session_state.market_data_loaded:
        data = st.session_state.yelp_market_data
        visualizer = YelpMarketVisualizer(data)
        
        # Choix du type d'analyse
        st.header("📊 Type d'analyse")
        analysis_type = st.selectbox(
            "Choisissez le type d'analyse:",
            [
                "Vue d'ensemble du marché",
                "Analyse par localisation",
                "Opportunités de marché",
                "Analyse des catégories",
                "Tendances temporelles",
                "Analyse business spécifique"
            ]
        )
        
        st.markdown("---")
        
        if analysis_type == "Vue d'ensemble du marché":
            st.header("🌍 Vue d'ensemble du marché")
            
            # Métriques générales
            if data.market_overview is not None and not data.market_overview.empty:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_businesses = data.market_overview['total_businesses'].sum()
                    st.metric("Total entreprises", f"{total_businesses:,}")
                
                with col2:
                    avg_rating = data.market_overview['avg_market_rating'].mean()
                    st.metric("Note moyenne globale", f"{avg_rating:.2f}/5")
                
                with col3:
                    active_businesses = data.market_overview['active_businesses'].sum()
                    st.metric("Entreprises actives", f"{active_businesses:,}")
                
                with col4:
                    avg_activity = data.market_overview['activity_rate'].mean()
                    st.metric("Taux d'activité moyen", f"{avg_activity:.1f}%")
            
            # Graphiques
            tab1, tab2 = st.tabs(["Localisations", "Vue d'ensemble"])
            
            with tab1:
                chart = visualizer.create_market_locations_chart()
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
                else:
                    st.warning("Aucune donnée de localisation disponible")
            
            with tab2:
                chart = visualizer.create_market_overview_chart()
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
                else:
                    st.warning("Aucune donnée de vue d'ensemble disponible")
        
        elif analysis_type == "Analyse par localisation":
            st.header("📍 Analyse par localisation")
            
            # Sélection de la localisation
            locations = data.get_market_locations_list()
            if locations:
                location_names = [loc['display_name'] for loc in locations]
                selected_location = st.selectbox("Choisissez une localisation:", location_names)
                
                # Trouver la localisation sélectionnée
                selected_loc_data = next(loc for loc in locations if loc['display_name'] == selected_location)
                
                # Métriques de la localisation
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Entreprises", selected_loc_data['business_count'])
                
                with col2:
                    st.metric("Note moyenne", f"{selected_loc_data['avg_rating']:.2f}/5")
                
                with col3:
                    st.metric("Total avis", f"{selected_loc_data['total_reviews']:,}")
                
                with col4:
                    st.metric("Utilisateurs uniques", f"{selected_loc_data['unique_reviewers']:,}")
                
                # Graphiques spécifiques à la localisation
                tab1, tab2, tab3 = st.tabs(["Vue d'ensemble", "Distribution des notes", "Tendances"])
                
                with tab1:
                    chart = visualizer.create_market_overview_chart(selected_location)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                
                with tab2:
                    chart = visualizer.create_rating_distribution_market_chart(selected_location)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                
                with tab3:
                    chart = visualizer.create_market_trends_chart(selected_location)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
            else:
                st.warning("Aucune donnée de localisation disponible")
        
        elif analysis_type == "Opportunités de marché":
            st.header("💡 Opportunités de marché")
            
            if data.market_opportunities is not None and not data.market_opportunities.empty:
                # Top opportunités
                st.subheader("🏆 Meilleures opportunités")
                top_opportunities = data.market_opportunities.head(10)
                
                for _, opp in top_opportunities.iterrows():
                    with st.expander(f"🎯 {opp['category']} à {opp['location_key']} (Score: {opp['opportunity_score']:.1f})"):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Concurrence", f"{opp['business_count']} entreprises")
                        
                        with col2:
                            st.metric("Qualité actuelle", f"{opp['avg_rating']:.1f}/5")
                        
                        with col3:
                            st.metric("Demande", f"{opp['total_reviews']:,} avis")
                        
                        st.info(f"**Type:** {opp['opportunity_type']}")
                        st.write(f"**Description:** {opp['description']}")
                
                # Graphique des opportunités
                st.subheader("📊 Visualisation des opportunités")
                chart = visualizer.create_opportunities_chart()
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
            else:
                st.warning("Aucune opportunité de marché disponible")
        
        elif analysis_type == "Analyse des catégories":
            st.header("🏷️ Analyse des catégories")
            
            if data.market_categories is not None and not data.market_categories.empty:
                # Graphique des catégories
                chart = visualizer.create_market_categories_chart()
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
                
                # Tableau des catégories
                st.subheader("📋 Détail des catégories")
                
                # Filtres
                col1, col2 = st.columns(2)
                with col1:
                    saturation_filter = st.selectbox(
                        "Filtrer par saturation:",
                        ["Toutes"] + list(data.market_categories['saturation'].unique())
                    )
                
                with col2:
                    min_score = st.slider("Score d'opportunité minimum:", 0.0, 10.0, 0.0, 0.5)
                
                # Appliquer les filtres
                filtered_categories = data.market_categories.copy()
                if saturation_filter != "Toutes":
                    filtered_categories = filtered_categories[filtered_categories['saturation'] == saturation_filter]
                filtered_categories = filtered_categories[filtered_categories['opportunity_score'] >= min_score]
                
                st.dataframe(
                    filtered_categories[['category', 'business_count', 'avg_rating', 'total_reviews', 'saturation', 'opportunity_score']],
                    use_container_width=True
                )
            else:
                st.warning("Aucune donnée de catégorie disponible")
        
        elif analysis_type == "Tendances temporelles":
            st.header("📈 Tendances temporelles")
            
            if data.market_trends_summary is not None and not data.market_trends_summary.empty:
                # Métriques des tendances
                st.subheader("📊 Résumé des tendances")
                
                positive_trends = len(data.market_trends_summary[data.market_trends_summary['rating_trend'] > 0.1])
                negative_trends = len(data.market_trends_summary[data.market_trends_summary['rating_trend'] < -0.1])
                stable_trends = len(data.market_trends_summary) - positive_trends - negative_trends
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Marchés en amélioration", positive_trends, delta=f"+{positive_trends}")
                
                with col2:
                    st.metric("Marchés stables", stable_trends)
                
                with col3:
                    st.metric("Marchés en dégradation", negative_trends, delta=f"-{negative_trends}")
                
                # Graphique des tendances
                chart = visualizer.create_market_trends_chart()
                if chart:
                    st.plotly_chart(chart, use_container_width=True)
                
                # Tableau des interprétations
                st.subheader("🔍 Interprétations des tendances")
                st.dataframe(
                    data.market_trends_summary[['location_key', 'rating_trend', 'activity_trend', 'trend_interpretation']],
                    use_container_width=True
                )
            else:
                st.warning("Aucune donnée de tendance disponible")
        
        elif analysis_type == "Analyse business spécifique":
            st.header("🏢 Analyse business spécifique")
            
            # Cette section utilise les analyses business existantes
            businesses = data.get_business_list()
            if businesses:
                business_names = [b['display_name'] for b in businesses]
                selected_business = st.selectbox("Choisissez une entreprise:", business_names)
                
                selected_id = next(b['business_id'] for b in businesses if b['display_name'] == selected_business)
                
                # Récupérer les données business
                if data.business_overview is not None:
                    business_data = data.business_overview[data.business_overview['business_id'] == selected_id]
                    
                    if not business_data.empty:
                        business = business_data.iloc[0]
                        
                        # Métriques business
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Note moyenne", f"{business['average_rating']:.2f}/5")
                        
                        with col2:
                            st.metric("Total avis", business['total_reviews'])
                        
                        with col3:
                            st.metric("Statut", "🟢 Ouvert" if business['is_open'] else "🔴 Fermé")
                        
                        with col4:
                            st.metric("Tendance récente", f"{business.get('recent_average', 0):.2f}/5")
                        
                        # Informations
                        st.info(f"**Nom:** {business['name']}")
                        st.info(f"**Adresse:** {business.get('address', '')}, {business.get('city', '')}, {business.get('state', '')}")
                        st.info(f"**Catégories:** {business.get('categories', 'Non spécifié')}")
                    else:
                        st.warning("Données business non trouvées")
            else:
                st.warning("Aucune entreprise disponible")

if __name__ == "__main__":
    main()