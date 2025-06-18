from database.getDataFromDatabase import *
import streamlit as st

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("## 👤 Analyse de la Table Utilisateurs – `user_table`")
st.markdown("---")

st.markdown("### 🎯 Objectif")
st.markdown("Étudier les profils des utilisateurs pour comprendre s’ils influencent les notes attribuées (biais, sévérité, activité).")

st.markdown("### 🔍 Axes d’analyse proposés")

# 🔸 Activité de l’utilisateur
st.markdown("#### 📊 Activité (`yelping_since`, `fans`, `friends`)")
st.markdown(
    "Observer si les utilisateurs très actifs ou avec beaucoup d’amis/fans ont tendance à donner des notes plus sévères ou indulgentes."
)
# 👉 METTRE UN GRAPHIQUE ICI (histogramme ou boxplot de notes selon activité)

st.markdown("---")

# 🔸 Statut Elite
st.markdown("#### 🌟 Statut Elite (`elite`)")
st.markdown(
    "Comparer les notes données par les utilisateurs Elite vs non Elite, car les Elite peuvent avoir des critères différents."
)
# 👉 METTRE UN GRAPHIQUE ICI (comparaison note moyenne Elite vs non Elite)

st.markdown("---")

# 🔸 Biais potentiel des utilisateurs
st.markdown("#### ⚖️ Distribution des notes par utilisateur")
st.markdown(
    "Identifier les utilisateurs qui donnent systématiquement des notes très basses ou très hautes pour ajuster l’analyse."
)
# 👉 METTRE UN GRAPHIQUE ICI (distribution notes par utilisateur, boxplot ou histogramme)

st.markdown("---")

# 🧠 Synthèse
st.markdown("### 🧠 Synthèse")
st.info(
    "👉 Ces analyses permettent de mieux comprendre l’impact des profils utilisateurs sur les notes et d’ajuster les conclusions en fonction des biais détectés."
)

