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
class YelpMarketDatabaseData:
    def __init__(self):
        self.engine = get_db_connection()
        self.market_locations = None
        self.market_categories = None
        self.market_overview = None
        self.market_segments = None
        self.market_quarterly_trends = None
        self.market_trends_summary = None
        self.market_opportunities = None
        self.market_rating_distribution = None
        
    def load_all_data(self):
        """Charge toutes les données depuis la base PostgreSQL"""
        if self.engine is None:
            st.error("Impossible de se connecter à la base de données")
            return False
            
        try:
            # Chargement de toutes les tables d'analyse de marché
            self.market_locations = pd.read_sql("SELECT * FROM market_locations", self.engine)
            self.market_categories = pd.read_sql("SELECT * FROM market_categories", self.engine)
            self.market_overview = pd.read_sql("SELECT * FROM market_overview", self.engine)
            self.market_segments = pd.read_sql("SELECT * FROM market_segments", self.engine)
            self.market_quarterly_trends = pd.read_sql("SELECT * FROM market_quarterly_trends", self.engine)
            self.market_trends_summary = pd.read_sql("SELECT * FROM market_trends_summary", self.engine)
            self.market_opportunities = pd.read_sql("SELECT * FROM market_opportunities", self.engine)
            self.market_rating_distribution = pd.read_sql("SELECT * FROM market_rating_distribution", self.engine)
            
            return True
        except Exception as e:
            st.error(f"Erreur lors du chargement des données: {e}")
            return False
    
    def get_available_locations(self) -> List[Dict]:
        """Retourne les localisations disponibles"""
        if self.market_locations is None or self.market_locations.empty:
            return []
        
        return self.market_locations.sort_values('business_count', ascending=False).to_dict('records')
    
    def get_available_categories(self) -> List[str]:
        """Retourne les catégories disponibles"""
        if self.market_categories is None or self.market_categories.empty:
            return []
        
        return sorted(self.market_categories['category'].tolist())

# ================== COUCHE D'ANALYSE DE MARCHÉ ==================
class MarketDatabaseAnalyzer:
    def __init__(self, data: YelpMarketDatabaseData):
        self.data = data
        
    def analyze_market_overview(self, location: str = None, category: str = None) -> Dict:
        """Analyse générale du marché depuis la base"""
        if self.data.market_overview is None:
            return {'error': 'Aucune donnée de vue d\'ensemble disponible'}
        
        # Filtrer par localisation
        if location and location != 'Toutes les villes':
            overview_data = self.data.market_overview[
                self.data.market_overview['location_key'] == location
            ]
        else:
            # Agrégation globale
            overview_data = self.data.market_overview.agg({
                'total_businesses': 'sum',
                'active_businesses': 'sum',
                'total_reviews': 'sum',
                'avg_market_rating': 'mean',
                'recently_active_businesses': 'sum'
            }).to_frame().T
            overview_data['activity_rate'] = (overview_data['recently_active_businesses'] / overview_data['total_businesses'] * 100).iloc[0]
            overview_data['market_health'] = self._calculate_market_health(
                overview_data['avg_market_rating'].iloc[0],
                overview_data['activity_rate']
            )
        
        if overview_data.empty:
            return {'error': 'Aucune donnée trouvée pour cette localisation'}
        
        overview = overview_data.iloc[0]
        
        # Distribution des notes pour cette localisation
        rating_distribution = self._get_rating_distribution(location)
        
        # Tendances mensuelles pour cette localisation
        monthly_trends = self._get_monthly_trends(location, category)
        
        return {
            'total_businesses': int(overview.get('total_businesses', 0)),
            'active_businesses': int(overview.get('active_businesses', 0)),
            'total_reviews': int(overview.get('total_reviews', 0)),
            'avg_market_rating': round(float(overview.get('avg_market_rating', 0)), 2),
            'rating_distribution': rating_distribution,
            'monthly_trends': monthly_trends,
            'market_health': overview.get('market_health', 'Inconnu')
        }
    
    def _calculate_market_health(self, avg_rating: float, activity_rate: float) -> str:
        """Calcule la santé du marché"""
        if avg_rating >= 4.0 and activity_rate >= 60:
            return 'Excellent'
        elif avg_rating >= 3.5 and activity_rate >= 40:
            return 'Bon'
        elif avg_rating >= 3.0 and activity_rate >= 20:
            return 'Moyen'
        else:
            return 'Difficile'
    
    def _get_rating_distribution(self, location: str = None) -> Dict:
        """Obtient la distribution des notes pour une localisation"""
        if self.data.market_rating_distribution is None:
            return {}
        
        if location and location != 'Toutes les villes':
            dist_data = self.data.market_rating_distribution[
                self.data.market_rating_distribution['location_key'] == location
            ]
        else:
            # Agrégation globale
            dist_data = self.data.market_rating_distribution.groupby('rating')['review_count'].sum().reset_index()
        
        if dist_data.empty:
            return {}
        
        # Convertir en dictionnaire {rating: count}
        if location and location != 'Toutes les villes':
            distribution = {}
            for _, row in dist_data.iterrows():
                distribution[int(row['rating'])] = int(row['review_count'])
        else:
            distribution = dict(zip(dist_data['rating'], dist_data['review_count']))
        
        return distribution
    
    def _get_monthly_trends(self, location: str = None, category: str = None) -> List[Dict]:
        """Obtient les tendances mensuelles"""
        if self.data.market_quarterly_trends is None:
            return []
        
        trends_data = self.data.market_quarterly_trends.copy()
        
        if location and location != 'Toutes les villes':
            trends_data = trends_data[trends_data['location_key'] == location]
        
        if trends_data.empty:
            return []
        
        # Convertir les trimestres en format mensuel pour l'affichage
        monthly_trends = trends_data.groupby('quarter').agg({
            'avg_rating': 'mean',
            'review_count': 'sum',
            'active_businesses': 'sum'
        }).reset_index()
        
        monthly_trends['month_str'] = monthly_trends['quarter']
        
        return monthly_trends.to_dict('records')
    
    def analyze_market_segments(self, location: str = None) -> Dict:
        """Analyse des segments de marché depuis la base"""
        if self.data.market_segments is None:
            return {'segments': [], 'total_segments': 0}
        
        segments_data = self.data.market_segments.copy()
        
        if location and location != 'Toutes les villes':
            segments_data = segments_data[segments_data['location_key'] == location]
        else:
            # Agrégation par catégorie pour vue globale
            segments_data = segments_data.groupby('category').agg({
                'business_count': 'sum',
                'total_reviews': 'sum',
                'avg_rating': 'mean',
                'unique_reviewers': 'sum',
                'opportunity_score': 'mean'
            }).reset_index()
            
            # Recalculer la saturation pour la vue globale
            segments_data['saturation'] = segments_data['business_count'].apply(
                lambda x: 'Faible' if x < 30 else 'Moyenne' if x < 100 else 'Élevée'
            )
        
        if segments_data.empty:
            return {'segments': [], 'total_segments': 0}
        
        # Trier par score d'opportunité
        segments_data = segments_data.sort_values('opportunity_score', ascending=False)
        
        return {
            'segments': segments_data.head(15).to_dict('records'),
            'total_segments': len(segments_data)
        }
    
    def analyze_temporal_trends(self, location: str = None, category: str = None) -> Dict:
        """Analyse des tendances temporelles depuis la base"""
        if self.data.market_trends_summary is None:
            return {'error': 'Aucune donnée de tendance disponible'}
        
        trends_data = self.data.market_trends_summary.copy()
        
        if location and location != 'Toutes les villes':
            trends_data = trends_data[trends_data['location_key'] == location]
        
        if trends_data.empty:
            return {'error': 'Aucune donnée de tendance pour cette localisation'}
        
        # Données trimestrielles détaillées
        quarterly_data = self._get_quarterly_data(location)
        
        if len(trends_data) > 0:
            trend_summary = trends_data.iloc[0]
            
            return {
                'quarterly_data': quarterly_data,
                'trend_interpretation': trend_summary.get('trend_interpretation', 'Données insuffisantes'),
                'rating_trend': float(trend_summary.get('rating_trend', 0)),
                'activity_trend': float(trend_summary.get('activity_trend', 0))
            }
        else:
            return {
                'quarterly_data': quarterly_data,
                'trend_interpretation': 'Données insuffisantes pour analyser les tendances',
                'rating_trend': 0,
                'activity_trend': 0
            }
    
    def _get_quarterly_data(self, location: str = None) -> List[Dict]:
        """Obtient les données trimestrielles détaillées"""
        if self.data.market_quarterly_trends is None:
            return []
        
        quarterly_data = self.data.market_quarterly_trends.copy()
        
        if location and location != 'Toutes les villes':
            quarterly_data = quarterly_data[quarterly_data['location_key'] == location]
        else:
            # Agrégation globale par trimestre
            quarterly_data = quarterly_data.groupby('quarter').agg({
                'avg_rating': 'mean',
                'review_count': 'sum',
                'active_businesses': 'sum',
                'unique_users': 'sum'
            }).reset_index()
        
        quarterly_data['quarter_str'] = quarterly_data['quarter']
        
        return quarterly_data.to_dict('records')
    
    def get_market_opportunities(self, location: str = None) -> Dict:
        """Identifie les opportunités de marché depuis la base"""
        if self.data.market_opportunities is None:
            return {'opportunities': [], 'total_opportunities': 0}
        
        opportunities_data = self.data.market_opportunities.copy()
        
        if location and location != 'Toutes les villes':
            opportunities_data = opportunities_data[opportunities_data['location_key'] == location]
        
        if opportunities_data.empty:
            return {'opportunities': [], 'total_opportunities': 0}
        
        # Filtrer les vraies opportunités et trier
        good_opportunities = opportunities_data[
            opportunities_data['opportunity_score'] >= 4
        ].sort_values('opportunity_score', ascending=False)
        
        return {
            'opportunities': good_opportunities.head(10).to_dict('records'),
            'total_opportunities': len(good_opportunities)
        }

# ================== COUCHE DE VISUALISATION ==================
class MarketDatabaseVisualizer:
    def __init__(self, analyzer: MarketDatabaseAnalyzer):
        self.analyzer = analyzer
    
    def create_market_overview_chart(self, market_data: Dict):
        """Graphique de vue d'ensemble du marché"""
        if 'error' in market_data or not market_data.get('rating_distribution'):
            return None
            
        ratings = list(market_data['rating_distribution'].keys())
        counts = list(market_data['rating_distribution'].values())
        
        fig = px.bar(
            x=ratings,
            y=counts,
            title=f"Distribution des notes du marché ({market_data['total_reviews']:,} avis)",
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
    
    def create_segments_analysis_chart(self, segments_data: Dict):
        """Graphique d'analyse des segments"""
        if not segments_data.get('segments'):
            return None
        
        df = pd.DataFrame(segments_data['segments'][:10])  # Top 10
        
        # Graphique en bulles : concurrence vs opportunité
        fig = px.scatter(
            df,
            x='business_count',
            y='opportunity_score',
            size='total_reviews',
            color='avg_rating',
            hover_name='category',
            title='Analyse des segments de marché',
            labels={
                'business_count': 'Nombre d\'entreprises (concurrence)',
                'opportunity_score': 'Score d\'opportunité',
                'avg_rating': 'Note moyenne',
                'total_reviews': 'Volume d\'avis'
            },
            color_continuous_scale='RdYlGn',
            size_max=50
        )
        
        fig.update_layout(
            xaxis_title="Concurrence (nombre d'entreprises)",
            yaxis_title="Score d'opportunité"
        )
        
        return fig
    
    def create_temporal_trends_chart(self, trends_data: Dict):
        """Graphique des tendances temporelles"""
        if 'error' in trends_data or not trends_data.get('quarterly_data'):
            return None
        
        df = pd.DataFrame(trends_data['quarterly_data'])
        
        if df.empty:
            return None
        
        # Graphique avec deux axes Y
        fig = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=('Évolution temporelle du marché')
        )
        
        # Note moyenne
        fig.add_trace(
            go.Scatter(
                x=df['quarter_str'],
                y=df['avg_rating'],
                name='Note moyenne',
                line=dict(color='blue', width=3),
                mode='lines+markers'
            ),
            secondary_y=False,
        )
        
        # Activité (nombre d'avis)
        fig.add_trace(
            go.Bar(
                x=df['quarter_str'],
                y=df['review_count'],
                name='Nombre d\'avis',
                opacity=0.6,
                yaxis='y2'
            ),
            secondary_y=True,
        )
        
        fig.update_xaxes(title_text="Trimestre")
        fig.update_yaxes(title_text="Note moyenne", secondary_y=False)
        fig.update_yaxes(title_text="Nombre d'avis", secondary_y=True)
        
        return fig
    
    def create_opportunities_chart(self, opportunities_data: Dict):
        """Graphique des opportunités de marché"""
        if not opportunities_data.get('opportunities'):
            return None
        
        df = pd.DataFrame(opportunities_data['opportunities'])
        
        # Graphique en barres horizontales
        fig = px.bar(
            df,
            x='opportunity_score',
            y='category',
            orientation='h',
            color='opportunity_score',
            title='Top opportunités de marché',
            labels={'opportunity_score': 'Score d\'opportunité', 'category': 'Catégorie'},
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False
        )
        
        return fig

# ================== INTERFACE STREAMLIT ==================
def main():
    st.set_page_config(
        page_title="Analyse de Marché Yelp - Database",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Analyse de Marché Yelp (Database)")
    st.markdown("Explorez les tendances, segments et opportunités du marché depuis PostgreSQL")
    st.markdown("---")
    
    # Initialisation des données
    if 'market_db_data' not in st.session_state:
        st.session_state.market_db_data = YelpMarketDatabaseData()
        st.session_state.market_db_loaded = False
    
    # Chargement des données depuis la base
    if not st.session_state.market_db_loaded:
        with st.spinner("Chargement des données d'analyse de marché depuis PostgreSQL..."):
            if st.session_state.market_db_data.load_all_data():
                st.session_state.market_db_loaded = True
                st.success("Données d'analyse de marché chargées avec succès!")
            else:
                st.error("Impossible de charger les données d'analyse de marché")
                st.stop()
    
    # Sidebar pour les filtres
    with st.sidebar:
        st.header("🎯 Filtres d'analyse")
        
        if st.session_state.market_db_loaded:
            st.success("✅ Connexion PostgreSQL active")
            
            data = st.session_state.market_db_data
            
            if data.market_locations is not None:
                st.info(f"📍 {len(data.market_locations)} localisations")
            if data.market_categories is not None:
                st.info(f"🏷️ {len(data.market_categories)} catégories")
            if data.market_opportunities is not None:
                st.info(f"💡 {len(data.market_opportunities)} opportunités")
        
        # Filtres géographiques
        analyzer = MarketDatabaseAnalyzer(st.session_state.market_db_data)
        locations = analyzer.data.get_available_locations()
        
        st.subheader("📍 Localisation")
        location_options = ['Toutes les villes'] + [loc['display_name'] for loc in locations[:50]]
        selected_location = st.selectbox("Sélectionnez une ville:", location_options)
        location_filter = None if selected_location == 'Toutes les villes' else selected_location
        
        # Filtres par catégorie
        st.subheader("🏷️ Catégorie")
        categories = analyzer.data.get_available_categories()
        category_options = ['Toutes les catégories'] + categories[:30]
        selected_category = st.selectbox("Sélectionnez une catégorie:", category_options)
        category_filter = None if selected_category == 'Toutes les catégories' else selected_category
        
        st.markdown("---")
        st.text("🔗 Base: PostgreSQL")
        st.text(f"🏠 Host: {DB_CONFIG['host']}")
        st.text(f"📋 DB: {DB_CONFIG['dbname']}")
        
        if st.button("🔄 Recharger depuis la base"):
            st.session_state.market_db_loaded = False
            st.rerun()
    
    # Interface principale
    if st.session_state.market_db_loaded:
        visualizer = MarketDatabaseVisualizer(analyzer)
        
        # Vue d'ensemble du marché
        st.header("🌟 Vue d'ensemble du marché")
        
        market_overview = analyzer.analyze_market_overview(location_filter, category_filter)
        
        if 'error' not in market_overview:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Entreprises totales", f"{market_overview['total_businesses']:,}")
            
            with col2:
                st.metric("Entreprises actives", f"{market_overview['active_businesses']:,}")
            
            with col3:
                st.metric("Total avis", f"{market_overview['total_reviews']:,}")
            
            with col4:
                health_color = {
                    'Excellent': '🟢', 'Bon': '🟡', 'Moyen': '🟠', 'Difficile': '🔴'
                }.get(market_overview['market_health'], '⚪')
                st.metric("Santé du marché", f"{health_color} {market_overview['market_health']}")
            
            # Note moyenne avec contexte
            st.info(f"📊 **Note moyenne du marché :** {market_overview['avg_market_rating']}/5")
            
            # Graphique de distribution
            chart = visualizer.create_market_overview_chart(market_overview)
            if chart:
                st.plotly_chart(chart, use_container_width=True)
        
        else:
            st.warning(market_overview['error'])
        
        # Analyse des segments
        st.markdown("---")
        st.header("🎯 Analyse des segments de marché")
        
        segments_data = analyzer.analyze_market_segments(location_filter)
        
        if segments_data['segments']:
            # Graphique des segments
            chart = visualizer.create_segments_analysis_chart(segments_data)
            if chart:
                st.plotly_chart(chart, use_container_width=True)
            
            # Table des segments
            st.subheader("📋 Top segments par opportunité")
            segments_df = pd.DataFrame(segments_data['segments'][:10])
            
            # Sélectionner et renommer les colonnes
            display_columns = ['category', 'business_count', 'avg_rating', 'total_reviews', 'opportunity_score', 'saturation']
            available_columns = [col for col in display_columns if col in segments_df.columns]
            
            if available_columns:
                segments_display = segments_df[available_columns].copy()
                segments_display.columns = ['Catégorie', 'Nb entreprises', 'Note moyenne', 'Total avis', 'Score opportunité', 'Saturation'][:len(available_columns)]
                st.dataframe(segments_display, use_container_width=True, hide_index=True)
        
        else:
            st.warning("Aucun segment trouvé pour ces critères")
        
        # Tendances temporelles
        st.markdown("---")
        st.header("📈 Tendances temporelles")
        
        trends_data = analyzer.analyze_temporal_trends(location_filter, category_filter)
        
        if 'error' not in trends_data:
            # Afficher l'interprétation des tendances
            if trends_data['rating_trend'] > 0:
                st.success(f"📈 Amélioration de la qualité (+{trends_data['rating_trend']:.2f} étoiles)")
            elif trends_data['rating_trend'] < 0:
                st.warning(f"📉 Dégradation de la qualité ({trends_data['rating_trend']:.2f} étoiles)")
            else:
                st.info("📊 Qualité stable")
            
            if trends_data['activity_trend'] > 0:
                st.success(f"📈 Augmentation d'activité (+{trends_data['activity_trend']:.1f} avis/trimestre)")
            elif trends_data['activity_trend'] < 0:
                st.warning(f"📉 Baisse d'activité ({trends_data['activity_trend']:.1f} avis/trimestre)")
            else:
                st.info("📊 Activité stable")
            
            st.info(f"**Analyse :** {trends_data['trend_interpretation']}")
            
            # Graphique des tendances
            chart = visualizer.create_temporal_trends_chart(trends_data)
            if chart:
                st.plotly_chart(chart, use_container_width=True)
        
        else:
            st.warning(trends_data['error'])
        
        # Opportunités de marché
        st.markdown("---")
        st.header("💡 Opportunités de marché")
        
        opportunities = analyzer.get_market_opportunities(location_filter)
        
        if opportunities['opportunities']:
            # Graphique des opportunités
            chart = visualizer.create_opportunities_chart(opportunities)
            if chart:
                st.plotly_chart(chart, use_container_width=True)
            
            # Liste des opportunités
            st.subheader("🚀 Recommandations stratégiques")
            
            for i, opp in enumerate(opportunities['opportunities'][:5], 1):
                with st.expander(f"{i}. {opp['category']} - {opp.get('opportunity_type', 'Opportunité')}"):
                    st.write(f"**Description :** {opp.get('description', 'Opportunité identifiée')}")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Score d'opportunité", f"{opp['opportunity_score']:.1f}/10")
                    with col2:
                        st.metric("Concurrence", f"{opp['business_count']} entreprises")
                    with col3:
                        st.metric("Note moyenne actuelle", f"{opp['avg_rating']:.1f}/5")
        
        else:
            st.info("Aucune opportunité spécifique identifiée pour ces critères")
        
        # Insights et recommandations finales
        st.markdown("---")
        st.header("🎯 Recommandations stratégiques")
        
        if 'error' not in market_overview:
            recommendations = []
            
            # Recommandations basées sur la santé du marché
            if market_overview['market_health'] == 'Excellent':
                recommendations.append("✅ Marché mature et de qualité - Focus sur la différenciation")
            elif market_overview['market_health'] == 'Difficile':
                recommendations.append("⚠️ Marché difficile - Opportunité d'améliorer les standards")
            
            # Recommandations basées sur l'activité
            if market_overview['total_businesses'] > 0:
                activity_rate = (market_overview['active_businesses'] / market_overview['total_businesses']) * 100
                if activity_rate < 30:
                    recommendations.append("📢 Faible activité générale - Opportunité de marketing digital")
            
            # Recommandations basées sur les notes
            if market_overview['avg_market_rating'] < 3.5:
                recommendations.append("🎯 Standards de qualité bas - Fort potentiel d'amélioration")
            
            if not recommendations:
                recommendations.append("📊 Marché équilibré - Analyser les segments spécifiques pour identifier les opportunités")
            
            for rec in recommendations:
                st.info(rec)
    
    else:
        st.info("🔄 Chargement des données d'analyse de marché en cours...")

if __name__ == "__main__":
    main()