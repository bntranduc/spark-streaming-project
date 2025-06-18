from database.getDataFromDatabase import *
import streamlit as st

# Configuration de la page
st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------- INTRODUCTION ---------------------- #

# st.title("📊 Yelp Dashboard – Analyse des avis")

# st.markdown("# 📝 Problématique")
# st.markdown("**Qu’est-ce qui explique que certaines entreprises reçoivent de mauvaises notes ?**")

# st.markdown("---")

# st.markdown("## 🔍 Contexte")
# st.markdown("""
# Sur des plateformes comme **Yelp**, les utilisateurs notent leur expérience et laissent des commentaires.
# Cela influence directement la **réputation** et la **performance** d’une entreprise.

# Mais pourquoi certaines entreprises accumulent-elles les mauvaises notes ?  
# C’est ce que nous allons découvrir à travers l’exploration des données.  
# """)

# st.markdown("---")

# st.markdown("## 🎯 Objectif")
# st.markdown("*Analyser les données pour identifier les **facteurs explicatifs** des mauvaises évaluations reçues par certaines entreprises.*")

# st.markdown("---")

# st.markdown("## 🧩 Axes d’analyse")

# col1, col2 = st.columns(2)

# with col1:
#     st.markdown("### 🏢 Entreprises")
#     st.markdown("""
#     - Type d’activité  
#     - Localisation  
#     - Nombre d’avis  
#     - Heures d’ouverture  
#     """)

# with col2:
#     st.markdown("### 👤 Utilisateurs")
#     st.markdown("""
#     - Comportement de notation  
#     - Historique des avis  
#     - Effets saisonniers  
#     """)

# st.markdown("### 💬 Contenu des avis")
# st.markdown("""
# - Analyse de **sentiment**  
# - Fréquence de certains mots-clés  
# - Longueur des messages  
# """)

# st.markdown("### ⚖️ Comparaison entreprises bien vs mal notées")
# st.markdown("""
# - Facteurs discriminants  
# - Profils types d’entreprise selon leur score  
# """)

# st.markdown("---")

# st.markdown("## 💡 Intérêt de l’étude")
# st.markdown("""
# - Identifier des axes **d’amélioration** pour les entreprises  
# - Mieux **comprendre les attentes clients**  
# - Aider à la **prise de décision stratégique** basée sur les retours  
# """)

# st.markdown("---")
# st.markdown("## 🗃️ Présentation des données")

# # -----------------------
# # 📝 Review Table (statique)
# # -----------------------
# st.markdown("### 📝 Table : Avis des utilisateurs (`review_table`)")
# st.markdown("- **Nombre de lignes :** `87 523`")
# st.markdown("- **Nombre de colonnes :** `6`")

# with st.expander("📋 Colonnes de la table `review_table`"):
#     st.dataframe(pd.DataFrame({
#         "Nom de colonne": ["review_id", "user_id", "business_id", "stars", "text", "date"],
#         "Type": ["string", "string", "string", "int", "text", "date"]
#     }))

# st.markdown("""
# **Description** :  
# Contient les avis rédigés par les utilisateurs concernant les entreprises.  
# Chaque ligne correspond à une note (de 1 à 5 étoiles) accompagnée d’un commentaire libre.
# """)

# # -----------------------
# # 🏬 Business Table (statique)
# # -----------------------
# st.markdown("### 🏬 Table : Entreprises (`business_table`)")
# st.markdown("- **Nombre de lignes :** `12 148`")
# st.markdown("- **Nombre de colonnes :** `7`")

# with st.expander("📋 Colonnes de la table `business_table`"):
#     st.dataframe(pd.DataFrame({
#         "Nom de colonne": [
#             "business_id", "name", "address", "city", "categories", "stars", "is_open"
#         ],
#         "Type": ["string", "string", "string", "string", "string", "float", "bool"]
#     }))

# st.markdown("""
# **Description** :  
# Référence toutes les entreprises enregistrées sur Yelp.  
# On y trouve leurs coordonnées, catégorie d'activité, statut (ouvert/fermé), et leur note moyenne.
# """)

# # -----------------------
# # 👤 User Table (statique)
# # -----------------------
# st.markdown("### 👥 Table : Utilisateurs (`user_table`)")
# st.markdown("- **Nombre de lignes :** `45 219`")
# st.markdown("- **Nombre de colonnes :** `6`")

# with st.expander("📋 Colonnes de la table `user_table`"):
#     st.dataframe(pd.DataFrame({
#         "Nom de colonne": [
#             "user_id", "name", "review_count", "average_stars", "yelping_since", "elite"
#         ],
#         "Type": ["string", "string", "int", "float", "date", "bool"]
#     }))

# st.markdown("""
# **Description** :  
# Contient les profils utilisateurs, avec des informations telles que le nombre d’avis laissés, la moyenne des notes données, ou encore l’ancienneté sur Yelp.
# """)


# ---------------------- STATS GÉNÉRALES ---------------------- #

st.title("📊 Yelp Dashboard – Analyse des avis")

st.markdown("---")
st.markdown("## 📌 Statistiques Générales")

# Style custom pour les cards
card_style = """
    <div style="
        background-color: #f9f9f9;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        text-align: center;
    ">
        <div style='font-size: 24px; font-weight: bold; color: #555;'>{label}</div>
        <div style='font-size: 42px; font-weight: bold; color: #009933;'>{value}</div>
    </div>
"""

# Chargement des données avec gestion des erreurs
with st.spinner("⏳ Chargement des données depuis la base..."):
    try:
        reviews = query_db("SELECT * FROM review_table;")
    except Exception as e:
        reviews = None
        st.error("📉 Erreur lors du chargement des **notes**.")
        st.exception(e)

    try:
        business = query_db("SELECT * FROM business_table;")
    except Exception as e:
        business = None
        st.error("🏢 Erreur lors du chargement des **entreprises**.")
        st.exception(e)

    try:
        users = query_db("SELECT * FROM user_table;")
    except Exception as e:
        users = None
        st.error("👤 Erreur lors du chargement des **utilisateurs**.")
        st.exception(e)

# Vérification de la disponibilité des données
if (
    reviews is None or reviews.empty or
    business is None or business.empty or
    users is None or users.empty
):
    st.info("📭 Aucune donnée disponible pour les notes, entreprises ou utilisateurs.")
else:
    st.success("✅ Données chargées avec succès !")

    st.markdown("### 📊 Quelques chiffres clés")

    try:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(card_style.format(label="📝 Nombre d’avis", value=reviews.shape[0]), unsafe_allow_html=True)
        with col2:
            st.markdown(card_style.format(label="🏬 Entreprises", value=business.shape[0]), unsafe_allow_html=True)
        with col3:
            st.markdown(card_style.format(label="👥 Utilisateurs", value=users.shape[0]), unsafe_allow_html=True)
    except Exception as e:
        st.error("❌ Une erreur est survenue lors de l'affichage des statistiques.")
        st.exception(e)

# ---------------------- TABLEAUX DE DONNÉES ---------------------- #

    st.markdown("---")
    st.markdown("## 📄 Exploration des données brutes")

    try:
        st.markdown("### 🏢 Avis des utilisateurs")
        st.dataframe(reviews)
    except Exception as e:
        st.error("🙁 Impossible d'afficher les avis.")
        st.exception(e)

    try:
        st.markdown("### 🏬 Détails des entreprises")
        st.dataframe(business)
    except Exception as e:
        st.error("⚠️ Problème lors de l'affichage des entreprises.")
        st.exception(e)

    try:
        st.markdown("### 👥 Informations sur les utilisateurs")
        st.dataframe(users)
    except Exception as e:
        st.error("😅 Les utilisateurs sont inaccessibles pour le moment.")
        st.exception(e)
