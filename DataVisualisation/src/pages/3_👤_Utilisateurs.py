from database.getDataFromDatabase import *
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# 👤 Analyse des Utilisateurs")
st.markdown("---")

# st.markdown("### Objectif")
# st.markdown("Étudier les profils des utilisateurs pour comprendre s’ils influencent les notes attribuées (biais, sévérité, activité).")

# st.markdown("### Axes d’analyse proposés")

# #  Activité de l’utilisateur
# st.markdown("#### Activité (`yelping_since`, `fans`, `friends`)")
# st.markdown(
#     "Observer si les utilisateurs très actifs ou avec beaucoup d’amis/fans ont tendance à donner des notes plus sévères ou indulgentes."
# )
# # METTRE UN GRAPHIQUE ICI (histogramme ou boxplot de notes selon activité)

# st.markdown("---")

# #  Statut Elite
# st.markdown("#### Statut Elite (`elite`)")
# st.markdown(
#     "Comparer les notes données par les utilisateurs Elite vs non Elite, car les Elite peuvent avoir des critères différents."
# )
# #  METTRE UN GRAPHIQUE ICI (comparaison note moyenne Elite vs non Elite)

# st.markdown("---")

# #  Biais potentiel des utilisateurs
# st.markdown("#### Distribution des notes par utilisateur")
# st.markdown(
#     "Identifier les utilisateurs qui donnent systématiquement des notes très basses ou très hautes pour ajuster l’analyse."
# )
# #  METTRE UN GRAPHIQUE ICI (distribution notes par utilisateur, boxplot ou histogramme)

# with st.spinner("Chargement des statistiques utilisateur..."):
#     try:
#         user_stats = query_db("SELECT * FROM user_table;")
#     except Exception as e:
#         user_stats = None
#         st.error(" Impossible de charger les données utilisateur.")
#         st.exception(e)

# if user_stats is None or user_stats.empty:
#     st.info("Aucune statistique utilisateur disponible.")
# else:
#     try:
#         st.markdown("#####  Distribution des moyennes de notes par utilisateur")

#         fig, ax = plt.subplots(figsize=(10, 4))
#         ax.hist(user_stats["avg_stars"], bins=30, color="#2C8F9B", edgecolor='black')
#         ax.set_title("Distribution des moyennes de notes par utilisateur", fontsize=14)
#         ax.set_xlabel("Note moyenne", fontsize=12)
#         ax.set_ylabel("Nombre d'utilisateurs", fontsize=12)
#         ax.grid(axis='y', linestyle='--', alpha=0.7)
#         st.pyplot(fig)

#         st.markdown("#####  Exemple de profils d’utilisateurs très sévères")
#         bad_raters = user_stats[user_stats["avg_stars"] < 2.5].sort_values(by="avg_stars")
#         st.dataframe(bad_raters[["user_id", "avg_stars", "total_reviews"]].head(10))

#     except Exception as e:
#         st.error("Une erreur est survenue pendant l'affichage du graphique ou du tableau.")
#         st.exception(e)




# st.markdown("---")

# # Synthèse
# st.markdown("### Synthèse")
# st.info(
#     "Ces analyses permettent de mieux comprendre l’impact des profils utilisateurs sur les notes et d’ajuster les conclusions en fonction des biais détectés."
# )

