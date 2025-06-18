from database.getDataFromDatabase import *
import streamlit as st

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# Bienvenu sur Yelp Dashboard ! 👋")
st.sidebar.header("🌟 Général")

st.write("""
Bienvenue sur le **Yelp Dashboard**, une application interactive conçue pour explorer les **raisons derrière les mauvaises notes** données aux entreprises sur Yelp.  
""")

# # ---------- VISUALISATIONS ----------

# Style des cards
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

# ################################################
st.markdown("---")
st.markdown("### 📌 Statistiques Générales")

with st.spinner("⏳ Chargement des données..."):
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

# Vérification des données
if (
    reviews is None or reviews.empty or
    business is None or business.empty or
    users is None or users.empty
):
    st.info("📭 Aucune donnée disponible pour les notes, entreprises ou utilisateurs.")
else:
    try:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(card_style.format(label="📝 Notes", value=reviews.shape[0]), unsafe_allow_html=True)
        with col2:
            st.markdown(card_style.format(label="🏬 Entreprises", value=business.shape[0]), unsafe_allow_html=True)
        with col3:
            st.markdown(card_style.format(label="👥 Utilisateurs", value=users.shape[0]), unsafe_allow_html=True)
    except Exception as e:
        st.error("❌ Une erreur est survenue lors de l'affichage des statistiques.")

    # --- Affichage des reviews ---
    try:
        st.markdown("---")
        st.markdown("## 🏢 **Les Notes**")
        st.dataframe(reviews)
    except Exception as e:
        st.error("🙁 Impossible d'afficher les notes pour le moment. Une erreur s'est produite. 📉")
        st.exception(e)

    # --- Affichage des entreprises ---
    try:
        st.markdown("---")
        st.markdown("## 🏬 **Entreprises**")
        st.dataframe(business)
    except Exception as e:
        st.error("😓 Oups ! Problème lors de l'affichage des données des entreprises. Vérifie les données ou réessaye plus tard. 🏢")
        st.exception(e)

    # --- Affichage des utilisateurs ---
    try:
        st.markdown("---")
        st.markdown("## 👤 **Utilisateurs**")
        st.dataframe(users)
    except Exception as e:
        st.error("😅 Les utilisateurs font grève... Impossible d'afficher leurs infos pour l'instant. 👥")
        st.exception(e)
