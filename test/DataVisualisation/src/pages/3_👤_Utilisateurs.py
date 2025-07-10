from database.getDataFromDatabase import *
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des utilisateurs",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# 👥 Analyse des Utilisateurs critiques")
st.markdown("---")
st.markdown("## Objectif")
st.markdown("##### Comprendre le profil des utilisateurs qui donnent des avis négatifs et identifier les patterns comportementaux liés aux mauvaises notes.")
st.markdown("## Axes d'analyse :")
st.markdown("""
Voici les axes étudiés pour mieux comprendre le comportement des utilisateurs donnant des avis négatifs :
1. **Profil des utilisateurs critiques**  
   - Analyser les caractéristiques des utilisateurs qui donnent fréquemment des notes ≤ 2★
2. **Impact de l'ancienneté sur la sévérité des notes**  
   - Étudier si les utilisateurs expérimentés sont plus ou moins sévères dans leurs évaluations
3. **Comportement des "power users" vs utilisateurs occasionnels**  
   - Comparer les patterns de notation selon le niveau d'activité des utilisateurs
---
""")

st.markdown("---")
st.markdown("### 1 - Distribution des utilisateurs par nombre de reviews")
st.markdown("""
**Pourquoi ce graphique ?**
- Comprendre la **répartition de l'activité des utilisateurs**.
- Identifier les **profils d'utilisateurs** (occasionnels vs. très actifs).
- Corréler le nombre de reviews avec la sévérité des notes.
""")

with st.spinner("Chargement de la distribution des utilisateurs..."):
    try:
        users_distribution = query_db("SELECT * FROM users_by_review_count_distribution;")
    except Exception as e:
        st.error("Impossible de charger les données depuis la base.")
        st.exception(e)
        users_distribution = None

if users_distribution is None or users_distribution.empty:
    st.info("Aucune donnée trouvée pour les utilisateurs. Veuillez vérifier la base.")
else:
    try:
        # Création du graphique à barres
        fig = px.bar(users_distribution, 
                     x="review_range", 
                     y="nb_users",
                     title="Nombre d'utilisateurs par catégorie de reviews",
                     labels={"nb_users": "Nombre d'utilisateurs", "review_range": "Nombre de reviews"})
        st.plotly_chart(fig)

        # Calcul des indicateurs clés
        total_users = users_distribution["nb_users"].sum()
        occasional_users = users_distribution[
            users_distribution["review_range"].isin(["1-5 reviews", "6-10 reviews"])
        ]["nb_users"].sum()
        active_users = users_distribution[
            ~users_distribution["review_range"].isin(["1-5 reviews", "6-10 reviews"])
        ]["nb_users"].sum()

        # Affichage des indicateurs
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Utilisateurs occasionnels (≤10 reviews)", 
                     f"{occasional_users:,}",
                     f"{(occasional_users/total_users)*100:.1f}% du total")
        
        with col2:
            st.metric("Utilisateurs actifs (>10 reviews)", 
                     f"{active_users:,}",
                     f"{(active_users/total_users)*100:.1f}% du total")

        # Tableau détaillé avec les moyennes
        st.markdown("**Détails par catégorie:**")
        st.dataframe(users_distribution.style.format({
            "avg_rating_given": "{:.2f}",
            "avg_low_rating_ratio": "{:.2%}"
        }))

        # Analyse complémentaire
        st.markdown("""
        **Observations possibles:**
        - Les utilisateurs occasionnels donnent-ils des notes plus sévères que les actifs ?
        - Y a-t-il une corrélation entre le nombre de reviews et la proportion de mauvaises notes ?
        """)

    except Exception as e:
        st.error("Une erreur est survenue lors de la génération du graphique.")
        st.exception(e)
        
st.markdown("---")
st.markdown("### 2 - Analyse des utilisateurs sévères")
st.markdown("""
**Pourquoi cette analyse ?**
- Identifier les **utilisateurs systématiquement sévères** (moyenne ≤ 2.5★)
- Comprendre leur **poids dans l'écosystème** global
- Analyser leur **modèle d'engagement** (nombre de reviews)
""")

with st.spinner("Chargement des statistiques des utilisateurs sévères..."):
    try:
        # Chargement des données
        severity_dist = query_db("SELECT * FROM users_by_severity_distribution;")
        severe_stats = query_db("SELECT * FROM severe_users_stats;")
        
        # Calculs complémentaires
        total_users = severity_dist["nb_users"].sum()
        total_reviews = severity_dist["total_reviews"].sum()
    except Exception as e:
        st.error("Impossible de charger les données depuis la base.")
        st.exception(e)
        severity_dist, severe_stats = None, None

if severity_dist is None or severe_stats is None:
    st.info("Aucune donnée trouvée. Veuillez vérifier la base.")
else:
    try:
        # 1. Graphique de répartition
        fig1 = px.pie(severity_dist, 
                     values="nb_users", 
                     names="severity_category",
                     title="Répartition des utilisateurs par sévérité de notation")
        st.plotly_chart(fig1)

        # 2. Indicateurs clés
        severe_users = int(severe_stats.iloc[0]["nb_severe_users"])
        severe_reviews = int(severe_stats.iloc[0]["total_reviews_by_severe_users"])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Utilisateurs sévères", 
                    f"{severe_users:,}",
                    f"{(severe_users/total_users)*100:.1f}% du total")
        
        with col2:
            st.metric("Reviews par utilisateur sévère", 
                     f"{severe_stats.iloc[0]['avg_reviews_per_severe_user']:.1f}")

        with col3:
            st.metric("Poids des reviews sévères", 
                    f"{severe_reviews:,}",
                    f"{(severe_reviews/total_reviews)*100:.1f}% du total")

        # 3. Tableau détaillé
        st.markdown("**Détails par catégorie de sévérité:**")
        st.dataframe(severity_dist.style.format({
            "avg_reviews_per_user": "{:.1f}",
            "avg_low_rating_ratio": "{:.2%}",
            "total_reviews": "{:,}"
        }))

        # 4. Analyse complémentaire
        st.markdown(f"""
        **Observations:**
        - Les utilisateurs sévères représentent **{severe_users/total_users*100:.1f}%** des utilisateurs
        - Mais ils sont responsables de **{severe_reviews/total_reviews*100:.1f}%** des reviews
        - Chaque utilisateur sévère poste en moyenne **{severe_stats.iloc[0]['avg_reviews_per_severe_user']:.1f}** reviews
        - **{severity_dist[severity_dist['severity_category'] == 'Très sévère (≤1.5★)'].iloc[0]['nb_users']}** utilisateurs donnent une moyenne ≤ 1.5★
        """)

    except Exception as e:
        st.error("Une erreur est survenue lors de l'analyse.")
        st.exception(e)

st.markdown("### 3 - Utilisateurs Polarisés (1★ ou 5★)")
st.markdown("""
**Pourquoi ?**  
- Identifie les utilisateurs **émotionnels** (peu nuancés).  
- Utile pour détecter les **fake reviews** (trop extrêmes).  
- Peut révéler des **biais culturels** (certaines cultures notent plus en extrêmes).  
""")

polarized_users = query_db("SELECT * FROM polarized_users ORDER BY polarization_score DESC;")

if not polarized_users.empty:
    st.metric("Utilisateurs Polarisés Détectés", len(polarized_users))
    
    fig = px.scatter(
        polarized_users, 
        x="avg_stars", 
        y="total_reviews",
        color="polarization_score",
        hover_name="user_id",
        title="Profils Polarisés (1★ ou 5★ dominants)"
    )
    st.plotly_chart(fig)
    
    st.markdown("**Top 5 Utilisateurs les Plus Polarisés**")
    st.dataframe(polarized_users.head(5))
else:
    st.warning("Aucun utilisateur polarisé détecté.")


st.markdown("### 4 - Utilisateurs Influents (Reviews Très Utiles)")
st.markdown("""
**Pourquoi ?**  
- Identifie les **meneurs d'opinion** (leurs avis impactent les autres).  
- Permet de **booster les bons contributeurs**.  
- Aide à modérer les **utilisateurs "fake"** (si utile mais notes étranges).  
""")

influential_users = query_db("SELECT * FROM influential_users;")

if not influential_users.empty:
    st.metric("Influenceurs Détectés", len(influential_users))
    
    fig = px.bar(
        influential_users.head(10), 
        x="user_id", 
        y="useful_count",
        title="Top 10 Utilisateurs les Plus Utiles"
    )
    st.plotly_chart(fig)
else:
    st.info("Aucun utilisateur influent détecté.")

st.markdown("### Serial Offenders (Cible Multiples Établissements)")
st.markdown("""
**Pourquoi ?**  
- Détecte les **trolls** ou **concurrents malveillants**.  
- Aide à **protéger les business** victimes de campagnes de dénigrement.  
""")

offenders = query_db("SELECT * FROM serial_offenders ORDER BY targeted_businesses DESC;")

if not offenders.empty:
    st.metric("Serial Offenders Détectés", len(offenders))
    
    st.dataframe(offenders)
else:
    st.success("Aucun serial offender détecté.")