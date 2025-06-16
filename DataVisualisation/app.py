from streamlit_autorefresh import st_autorefresh
from src.getDataFromDatabase import *
import streamlit as st

st_autorefresh(interval=1000)


st.title("📝 Dashboard Yelp")
st.markdown("---")

# ---------- VISUALISATIONS ----------

# Récupération des données
all_user = query_db("SELECT COUNT(*) FROM user_table;")
all_review = query_db("SELECT COUNT(*) FROM review_table;")
all_query_business = query_db("SELECT COUNT(*) FROM business_table;")

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

# Layout avec colonnes
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(card_style.format(label="Utilisateurs", value=all_user['count'][0]), unsafe_allow_html=True)

with col2:
    st.markdown(card_style.format(label="Reviews", value=all_review['count'][0]), unsafe_allow_html=True)

with col3:
    st.markdown(card_style.format(label="Entreprises", value=all_query_business['count'][0]), unsafe_allow_html=True)


