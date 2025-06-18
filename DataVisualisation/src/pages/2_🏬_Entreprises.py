from database.getDataFromDatabase import *
import streamlit as st

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("## 🏢 Analyse de la Table Entreprises – `business_table`")
st.markdown("---")

st.markdown("### 🎯 Objectif")
st.markdown("Comprendre les caractéristiques des entreprises qui pourraient influencer leurs notes, notamment celles qui reçoivent des avis négatifs.")

st.markdown("### 🔍 Axes d’analyse proposés")

# 🔸 Type d’activité
st.markdown("#### 🏷️ Type d’activité (`categories`)")
st.markdown(
    "Identifier si certains types d’entreprises (restaurants, services, commerces) sont plus susceptibles de recevoir de mauvaises notes."
)
# 👉 METTRE UN GRAPHIQUE ICI (bar chart des catégories vs note moyenne)

st.markdown("---")

# 🔸 Localisation géographique
st.markdown("#### 📍 Localisation (`city`, `state`)")
st.markdown(
    "Analyser si certaines zones géographiques présentent plus de mauvaises notes, pour détecter des facteurs régionaux."
)
# 👉 METTRE UN GRAPHIQUE ICI (carte ou heatmap des notes par ville/état)

st.markdown("---")

# 🔸 Statut d’ouverture
st.markdown("#### 🚪 Statut d’ouverture (`is_open`)")
st.markdown(
    "Vérifier si les entreprises fermées reçoivent plus de mauvaises notes, ce qui pourrait indiquer des problèmes liés à la qualité."
)
# 👉 METTRE UN GRAPHIQUE ICI (comparaison notes moyennes entreprises ouvertes vs fermées)

st.markdown("---")

# 🔸 Ancienneté et volume d’avis
st.markdown("#### ⏳ Ancienneté et nombre d’avis")
st.markdown(
    "Étudier si les entreprises plus anciennes ou avec plus d’avis ont tendance à mieux ou moins bien être notées."
)
# 👉 METTRE UN GRAPHIQUE ICI (scatter plot ancienneté vs note moyenne, ou nb avis vs note)

st.markdown("---")

# 🧠 Synthèse
st.markdown("### 🧠 Synthèse")
st.info(
    "👉 Ces analyses aident à repérer les facteurs liés à l’entreprise qui influencent la satisfaction client, et à cibler les causes structurelles des mauvaises notes."
)
