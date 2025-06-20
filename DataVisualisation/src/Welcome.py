from database.getDataFromDatabase import *
import streamlit as st

# Configuration de la page
st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Yelp Dashboard – Analyse des avis")

st.markdown("---")
st.markdown("## Statistiques Générales")

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

with st.spinner("Chargement des données depuis la base..."):
    try:
        reviews = query_db("SELECT * FROM review_table;")
    except Exception as e:
        reviews = None
        st.error("Erreur lors du chargement des **notes**.")
        st.exception(e)

    try:
        business = query_db("SELECT * FROM business_table;")
    except Exception as e:
        business = None
        st.error("Erreur lors du chargement des **entreprises**.")
        st.exception(e)

    try:
        users = query_db("SELECT * FROM user_table;")
    except Exception as e:
        users = None
        st.error("Erreur lors du chargement des **utilisateurs**.")
        st.exception(e)

# Vérification de la disponibilité des données
if (
    reviews is None or reviews.empty or
    business is None or business.empty or
    users is None or users.empty
):
    st.info("Aucune donnée disponible pour les notes, entreprises ou utilisateurs.")
else:
    st.success("Données chargées avec succès !")

    st.markdown("### Quelques chiffres clés")

    try:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(card_style.format(label="Nombre d’avis", value=reviews.shape[0]), unsafe_allow_html=True)
        with col2:
            st.markdown(card_style.format(label="Entreprises", value=business.shape[0]), unsafe_allow_html=True)
        with col3:
            st.markdown(card_style.format(label="Utilisateurs", value=users.shape[0]), unsafe_allow_html=True)
    except Exception as e:
        st.error("Une erreur est survenue lors de l'affichage des statistiques.")
        st.exception(e)

# ---------------------- TABLEAUX DE DONNÉES ---------------------- #

    # st.markdown("---")
    # st.markdown("## Exploration des données brutes")

    # try:
    #     st.markdown("### Avis des utilisateurs")
    #     st.dataframe(reviews)
    # except Exception as e:
    #     st.error("Impossible d'afficher les avis.")
    #     st.exception(e)

    # try:
    #     st.markdown("### Détails des entreprises")
    #     st.dataframe(business)
    # except Exception as e:
    #     st.error("Problème lors de l'affichage des entreprises.")
    #     st.exception(e)

    # try:
    #     st.markdown("### Informations sur les utilisateurs")
    #     st.dataframe(users)
    # except Exception as e:
    #     st.error("Les utilisateurs sont inaccessibles pour le moment.")
    #     st.exception(e)

st.markdown("## Problématique")
st.markdown(
    "Déterminer les entrerprise pour les raison pour les quels les entreprise recoivement de movaise ?"
    "Comment"
)
