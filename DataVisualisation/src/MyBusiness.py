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

# ================== COUCHE DE DONNÉES ==================
class YelpDatabaseData:
    def __init__(self):
        self.engine = get_db_connection()
        self.business_count = None
        self.review_count = None
        self.user_count = None
        
        self.business_overview = None
        self.rating_distribution = None
        self.temporal_analysis = None
        self.trend_analysis = None
        self.business_user_stats = None
        self.top_reviewers = None
        
    def load_all_data(self):
        """Charge toutes les données depuis la base PostgreSQL"""
        if self.engine is None:
            st.error("Impossible de se connecter à la base de données")
            return False
            
        try:
            # Chargement de toutes les tables
            
            self.business_count = pd.read_sql("SELECT COUNT(*) as count FROM business_table", self.engine).iloc[0]['count']
            self.review_count = pd.read_sql("SELECT COUNT(*) as count FROM review_table", self.engine).iloc[0]['count']
            self.user_count = pd.read_sql("SELECT COUNT(*) as count FROM user_table", self.engine).iloc[0]['count']

            self.business_overview = pd.read_sql("SELECT * FROM business_overview ORDER BY total_reviews DESC", self.engine)
            self.rating_distribution = pd.read_sql("SELECT * FROM rating_distribution", self.engine)
            self.temporal_analysis = pd.read_sql("SELECT * FROM temporal_analysis", self.engine)
            self.trend_analysis = pd.read_sql("SELECT * FROM trend_analysis", self.engine)
            self.business_user_stats = pd.read_sql("SELECT * FROM business_user_stats", self.engine)
            self.top_reviewers = pd.read_sql("SELECT * FROM top_reviewers", self.engine)
            
            return True
        except Exception as e:
            st.error(f"Erreur lors du chargement des données: {e}")
            return False
    
    def get_business_list(self) -> List[Dict]:
        """Retourne la liste des entreprises depuis business_overview"""
        if self.business_overview is None or self.business_overview.empty:
            return []
            
        businesses = self.business_overview[['business_id', 'name', 'city', 'state', 'categories']].copy()
        businesses['display_name'] = businesses['name'] + ' (' + businesses['city'] + ', ' + businesses['state'] + ')'
        
        return businesses.to_dict('records')

    def get_review_texts(self, business_id: str) -> pd.DataFrame:
        """Récupère les textes des avis pour une entreprise donnée"""
        if self.engine is None:
            return pd.DataFrame()
        
        # Méthode 1: F-string simple (plus sûre)
        try:
            # Échapper les apostrophes potentielles dans business_id
            safe_business_id = business_id.replace("'", "''")
            query = f"SELECT text FROM review_table WHERE business_id = '{safe_business_id}' AND text IS NOT NULL AND text != ''"
            review_df = pd.read_sql(query, self.engine)
            return review_df
        except Exception as e1:
            st.warning(f"Méthode 1 échouée: {e1}")
            
            # Méthode 2: Connexion directe avec paramètres nommés
            try:
                with self.engine.connect() as conn:
                    query = "SELECT text FROM review_table WHERE business_id = %(business_id)s AND text IS NOT NULL AND text != ''"
                    review_df = pd.read_sql(query, conn, params={'business_id': business_id})
                    return review_df
            except Exception as e2:
                st.warning(f"Méthode 2 échouée: {e2}")
                
                # Méthode 3: Utilisation de psycopg2 directement
                try:
                    import psycopg2
                    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
                    conn = psycopg2.connect(connection_string)
                    
                    query = "SELECT text FROM review_table WHERE business_id = %s AND text IS NOT NULL AND text != ''"
                    review_df = pd.read_sql(query, conn, params=(business_id,))
                    conn.close()
                    return review_df
                except Exception as e3:
                    st.error(f"Toutes les méthodes ont échoué. Dernière erreur: {e3}")
                    return pd.DataFrame()

            return pd.DataFrame()

# ================== COUCHE DE TRANSFORMATION ==================
class YelpDatabaseAnalyzer:
    def __init__(self, data: YelpDatabaseData):
        self.data = data
        
    def get_business_overview(self, business_id: str) -> Dict:
        """Vue d'ensemble d'une entreprise depuis la base"""
        if self.data.business_overview is None:
            return {}
            
        business_data = self.data.business_overview[
            self.data.business_overview['business_id'] == business_id
        ]
        
        if business_data.empty:
            return {'message': 'Entreprise non trouvée'}
            
        business = business_data.iloc[0]
        
        return {
            'name': business['name'],
            'address': f"{business.get('address', '')}, {business.get('city', '')}, {business.get('state', '')}",
            'categories': business.get('categories', 'Non spécifié'),
            'total_reviews': int(business.get('total_reviews', 0)),
            'average_rating': float(business.get('average_rating', 0)),
            'recent_average': float(business.get('recent_average', 0)),
            'is_open': bool(business.get('is_open', True)),
            'last_review_date': business.get('last_review_date', 'N/A')
        }
    
    def get_rating_distribution(self, business_id: str) -> Dict:
        """Distribution des notes depuis la base"""
        if self.data.rating_distribution is None:
            return {}
            
        rating_data = self.data.rating_distribution[
            self.data.rating_distribution['business_id'] == business_id
        ]
        
        if rating_data.empty:
            return {}
            
        # Convertir en dictionnaire {rating: count}
        distribution = {}
        for _, row in rating_data.iterrows():
            distribution[int(row['rating'])] = int(row['count'])
            
        return distribution
    
    def get_temporal_analysis(self, business_id: str, period_type: str = 'month') -> Dict:
        """Analyse temporelle depuis la base"""
        if self.data.temporal_analysis is None:
            return {'data': [], 'trend': 'stable'}
            
        temporal_data = self.data.temporal_analysis[
            (self.data.temporal_analysis['business_id'] == business_id) &
            (self.data.temporal_analysis['period_type'] == period_type)
        ].copy()
        
        if temporal_data.empty:
            return {'data': [], 'trend': 'stable'}
        
        # Trier par période
        temporal_data = temporal_data.sort_values('period')
        
        # Détection de tendance
        if len(temporal_data) >= 3:
            recent_avg = temporal_data['avg_rating'].tail(3).mean()
            older_avg = temporal_data['avg_rating'].head(3).mean()
            
            if recent_avg > older_avg + 0.2:
                trend = 'amélioration'
            elif recent_avg < older_avg - 0.2:
                trend = 'dégradation'
            else:
                trend = 'stable'
        else:
            trend = 'stable'
        
        # Renommer les colonnes pour compatibilité
        temporal_data['period_str'] = temporal_data['period']
        
        return {
            'data': temporal_data.to_dict('records'),
            'trend': trend
        }
    
    def get_trend_analysis(self, business_id: str) -> Dict:
        """Analyse des tendances depuis la base"""
        if self.data.trend_analysis is None:
            return {'trend': 'stable'}
            
        trend_data = self.data.trend_analysis[
            self.data.trend_analysis['business_id'] == business_id
        ]
        
        if trend_data.empty:
            return {'trend': 'stable'}
            
        trend_info = trend_data.iloc[0]
        
        return {
            'trend': trend_info.get('trend', 'stable'),
            'early_avg': float(trend_info.get('early_avg', 0)),
            'recent_avg': float(trend_info.get('recent_avg', 0)),
            'total_quarters': int(trend_info.get('total_quarters', 0))
        }
    
    def get_user_analysis(self, business_id: str) -> Dict:
        """Analyse des utilisateurs depuis la base"""
        if self.data.business_user_stats is None:
            return {'user_stats': {}, 'top_reviewers': []}
            
        # Stats générales
        user_stats_data = self.data.business_user_stats[
            self.data.business_user_stats['business_id'] == business_id
        ]
        
        if user_stats_data.empty:
            user_stats = {}
        else:
            stats = user_stats_data.iloc[0]
            user_stats = {
                'unique_users': int(stats.get('unique_users', 0)),
                'avg_reviews_per_user': float(stats.get('avg_reviews_per_user', 0)),
                'elite_users': int(stats.get('elite_users_count', 0))
            }
        
        # Top reviewers
        if self.data.top_reviewers is not None:
            top_reviewers_data = self.data.top_reviewers[
                self.data.top_reviewers['business_id'] == business_id
            ].sort_values('rn')
            
            top_reviewers = []
            for _, reviewer in top_reviewers_data.iterrows():
                top_reviewers.append({
                    'name': reviewer.get('name', 'Anonyme'),
                    'avg_rating': float(reviewer.get('avg_rating', 0)),
                    'review_count_business': int(reviewer.get('review_count', 0)),
                    'useful_votes': int(reviewer.get('useful_votes', 0)),
                    'elite': reviewer.get('elite', '')
                })
        else:
            top_reviewers = []
        
        return {
            'user_stats': user_stats,
            'top_reviewers': top_reviewers
        }

    def generate_wordcloud(self, business_id: str) -> plt.Figure:
        """Génère un nuage de mots à partir des avis d'une entreprise"""
        try:
            # Récupérer les textes des avis
            review_df = self.data.get_review_texts(business_id)
            
            if review_df is None or review_df.empty:
                return None
            
            # Combiner tous les textes
            all_text = " ".join(review_df['text'].dropna().astype(str).tolist())
            
            if not all_text.strip():
                return None
            
            # Nettoyer le texte
            all_text = all_text.lower()
            all_text = re.sub(r"[^a-zA-ZÀ-ÿ\s]", " ", all_text)
            all_text = re.sub(r"\s+", " ", all_text).strip()
            
            if len(all_text) < 10:  # Texte trop court
                return None
            
            # Créer le nuage de mots
            custom_stopwords = STOPWORDS.union({
                "restaurant", "place", "food", "eat", "go", "get", "really", 
                "good", "great", "nice", "time", "back", "come", "went", 
                "one", "two", "would", "could", "also", "much", "well",
                "like", "love", "want", "need", "think", "know", "see",
                "make", "take", "give", "way", "say", "come", "go"
            })
            
            wordcloud = WordCloud(
                width=800,
                height=400,
                background_color='white',
                stopwords=custom_stopwords,
                max_words=100,
                max_font_size=90,
                colormap="viridis",
                random_state=42,
                collocations=False
            ).generate(all_text)
            
            # Créer la figure matplotlib
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis('off')
            ax.set_title("Nuage de mots des avis", fontsize=16, pad=20)
            
            return fig
            
        except Exception as e:
            st.error(f"Erreur lors de la génération du nuage de mots: {e}")
            return None

# ================== COUCHE DE VISUALISATION ==================
class YelpDatabaseVisualizer:
    def __init__(self, analyzer: YelpDatabaseAnalyzer):
        self.analyzer = analyzer
    
    def create_rating_distribution_chart(self, business_id: str):
        """Graphique de distribution des notes"""
        distribution = self.analyzer.get_rating_distribution(business_id)
        
        if not distribution:
            return None
            
        ratings = list(distribution.keys())
        counts = list(distribution.values())
        
        fig = px.bar(
            x=ratings, 
            y=counts,
            title="Distribution des notes",
            labels={'x': 'Note (étoiles)', 'y': 'Nombre d\'avis'},
            color=ratings,
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_layout(
            xaxis_title="Note (étoiles)",
            yaxis_title="Nombre d'avis",
            showlegend=False
        )
        
        return fig
    
    def create_temporal_trend_chart(self, business_id: str, period: str = 'month'):
        """Graphique d'évolution temporelle"""
        temporal_data = self.analyzer.get_temporal_analysis(business_id, period)
        
        if not temporal_data['data']:
            return None
        
        df = pd.DataFrame(temporal_data['data'])
        
        # Graphique avec deux axes Y
        fig = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=('Évolution des performances')
        )
        
        # Note moyenne
        fig.add_trace(
            go.Scatter(
                x=df['period_str'],
                y=df['avg_rating'],
                name='Note moyenne',
                line=dict(color='blue', width=3),
                mode='lines+markers'
            ),
            secondary_y=False,
        )
        
        # Nombre d'avis
        fig.add_trace(
            go.Bar(
                x=df['period_str'],
                y=df['review_count'],
                name='Nombre d\'avis',
                opacity=0.6,
                yaxis='y2'
            ),
            secondary_y=True,
        )
        
        fig.update_xaxes(title_text="Période")
        fig.update_yaxes(title_text="Note moyenne", secondary_y=False)
        fig.update_yaxes(title_text="Nombre d'avis", secondary_y=True)
        
        return fig

# ================== INTERFACE STREAMLIT ==================
def main():
    st.set_page_config(
        page_title="Yelp Business Analytics - Database",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Yelp Business Analytics Dashboard (Database)")
    st.markdown("---")
    
    # Initialisation des données
    if 'yelp_db_data' not in st.session_state:
        st.session_state.yelp_db_data = YelpDatabaseData()
        st.session_state.db_data_loaded = False
    
    # Chargement des données depuis la base
    if not st.session_state.db_data_loaded:
        with st.spinner("Chargement des données depuis PostgreSQL..."):
            if st.session_state.yelp_db_data.load_all_data():
                st.session_state.db_data_loaded = True
                st.success("Données chargées avec succès depuis la base!")
            else:
                st.error("Impossible de charger les données depuis la base")
                st.stop()
    
    # Sidebar pour informations
    with st.sidebar:
        st.header("📊 Informations Database")
        
        if st.session_state.db_data_loaded:
            st.success("✅ Connexion PostgreSQL active")
            
            data = st.session_state.yelp_db_data
            
            if data.business_count is not None:
                st.info(f"📈 {data.business_count} entreprises")
            if data.review_count is not None:
                st.info(f"💬 {data.review_count} avis")
            if data.user_count is not None:
                st.info(f"👥 {data.user_count} utilisateurs")
        
        st.markdown("---")
        st.text("🔗 Base: PostgreSQL")
        st.text(f"🏠 Host: {DB_CONFIG['host']}")
        st.text(f"📋 DB: {DB_CONFIG['dbname']}")
        
        if st.button("🔄 Recharger depuis la base"):
            st.session_state.db_data_loaded = False
            st.rerun()
    
    # Interface principale
    if st.session_state.db_data_loaded:
        analyzer = YelpDatabaseAnalyzer(st.session_state.yelp_db_data)
        visualizer = YelpDatabaseVisualizer(analyzer)
        
        # Sélection de l'entreprise
        st.header("🏢 Sélection de l'entreprise")
        businesses = st.session_state.yelp_db_data.get_business_list()

        if businesses:
            business_names = [b['display_name'] for b in businesses]
            selected_business = st.selectbox(
                "Choisissez une entreprise:",
                business_names
            )
            
            # Trouver l'ID de l'entreprise sélectionnée
            selected_id = next(b['business_id'] for b in businesses if b['display_name'] == selected_business)
            
            # Affichage des analyses
            st.markdown("---")
            
            # Vue d'ensemble
            st.header("📈 Vue d'ensemble")
            overview = analyzer.get_business_overview(selected_id)
            
            if 'message' in overview:
                st.warning(overview['message'])
            else:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    delta_value = overview['recent_average'] - overview['average_rating'] if overview['recent_average'] != overview['average_rating'] else None
                    delta_str = f"{delta_value:.1f}" if delta_value is not None else None
                    
                    st.metric(
                        "Note moyenne",
                        f"{overview['average_rating']}/5",
                        delta_str
                    )
                
                with col2:
                    st.metric("Total des avis", overview['total_reviews'])
                
                with col3:
                    st.metric("Statut", "🟢 Ouvert" if overview['is_open'] else "🔴 Fermé")
                
                with col4:
                    st.metric("Tendance récente", f"{overview['recent_average']}/5")
                
                # Informations de l'entreprise
                st.info(f"**Adresse:** {overview['address']}")
                st.info(f"**Catégories:** {overview['categories']}")
                if overview.get('last_review_date'):
                    st.info(f"**Dernier avis:** {overview['last_review_date']}")
                
                # Graphiques
                st.markdown("---")
                st.header("📊 Analyses détaillées")
                
                tab1, tab2, tab3, tab4 = st.tabs(["Distribution des notes", "Évolution temporelle", "Tendances", "Nuage de mots"])
                
                with tab1:
                    chart = visualizer.create_rating_distribution_chart(selected_id)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.warning("Aucune donnée de distribution disponible")
                
                with tab2:
                    period = st.selectbox("Période d'analyse:", ['month', 'quarter', 'year'])
                    
                    chart = visualizer.create_temporal_trend_chart(selected_id, period)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                        
                        # Afficher la tendance depuis l'analyse temporelle
                        temporal_data = analyzer.get_temporal_analysis(selected_id, period)
                        if temporal_data['trend'] == 'amélioration':
                            st.success("📈 Tendance positive détectée!")
                        elif temporal_data['trend'] == 'dégradation':
                            st.warning("📉 Tendance négative détectée!")
                        else:
                            st.info("📊 Tendance stable")
                    else:
                        st.warning("Aucune donnée temporelle disponible")
                
                with tab3:
                    # Affichage de l'analyse des tendances depuis la base
                    trend_data = analyzer.get_trend_analysis(selected_id)
                    
                    if trend_data and 'trend' in trend_data:
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Tendance générale", trend_data['trend'])
                        
                        with col2:
                            if 'early_avg' in trend_data:
                                st.metric("Moyenne initiale", f"{trend_data['early_avg']:.2f}")
                        
                        with col3:
                            if 'recent_avg' in trend_data:
                                st.metric("Moyenne récente", f"{trend_data['recent_avg']:.2f}")
                        
                        if trend_data['trend'] == 'amélioration':
                            st.success("📈 L'entreprise montre une amélioration dans le temps!")
                        elif trend_data['trend'] == 'dégradation':
                            st.warning("📉 L'entreprise montre une dégradation dans le temps!")
                        else:
                            st.info("📊 L'entreprise maintient des performances stables")
                    else:
                        st.warning("Aucune analyse de tendance disponible")
                
                with tab4:
                    # Nuage de mots des avis
                    st.subheader("☁️ Nuage de mots des avis")
                    
                    with st.spinner("Génération du nuage de mots..."):
                        wordcloud_fig = analyzer.generate_wordcloud(selected_id)
                        
                        if wordcloud_fig:
                            st.pyplot(wordcloud_fig)
                            st.info("Ce nuage de mots représente les mots les plus fréquents dans les avis de cette entreprise.")
                        else:
                            st.warning("Aucun texte d'avis disponible pour générer le nuage de mots.")

                # Analyse des utilisateurs
                st.markdown("---")
                st.header("👥 Analyse des utilisateurs")
                
                user_data = analyzer.get_user_analysis(selected_id)
                
                if user_data['user_stats']:
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Utilisateurs uniques", user_data['user_stats']['unique_users'])
                    
                    with col2:
                        st.metric("Avis/utilisateur", f"{user_data['user_stats']['avg_reviews_per_user']:.1f}")
                    
                    with col3:
                        st.metric("Utilisateurs Elite", user_data['user_stats']['elite_users'])
                    
                    # Top reviewers
                    if user_data['top_reviewers']:
                        st.subheader("🏆 Top contributeurs")
                        df_reviewers = pd.DataFrame(user_data['top_reviewers'])
                        st.dataframe(df_reviewers, use_container_width=True)
                else:
                    st.warning("Aucune donnée utilisateur disponible")

        else:
            st.warning("Aucune entreprise trouvée dans la base de données")
    
    else:
        st.info("🔄 Chargement des données depuis PostgreSQL...")

if __name__ == "__main__":
    main()