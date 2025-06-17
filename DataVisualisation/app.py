from streamlit_autorefresh import st_autorefresh
from src.getDataFromDatabase import *
import streamlit as st
import plotly.express as px
import time
import numpy as np
# st_autorefresh(interval=1000)

st.title("📝 Dashboard Yelp")

# ---------- VISUALISATIONS ----------

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

################################################
st.markdown("---")
st.markdown("### 📌 Statistiques Générales")
with st.spinner("chargement de données ..."):
    all_user = query_db("SELECT COUNT(*) FROM user_table;")
    all_review = query_db("SELECT COUNT(*) FROM review_table;")
    all_query_business = query_db("SELECT COUNT(*) FROM business_table;")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(card_style.format(label="Utilisateurs", value=all_user['count'][0]), unsafe_allow_html=True)
    with col2:
        st.markdown(card_style.format(label="Reviews", value=all_review['count'][0]), unsafe_allow_html=True)
    with col3:
        st.markdown(card_style.format(label="Entreprises", value=all_query_business['count'][0]), unsafe_allow_html=True)

################################################
with st.spinner("chargement de données ..."):
    top_categories = query_db("SELECT * FROM top_categories_table;")
    if (len(top_categories) > 0):
        st.markdown("---")
        st.markdown("### 🥧 Analyse des catégories les plus fréquentes")
        fig = px.pie(top_categories, names='category', values='count')
        st.plotly_chart(fig)

#################################################
with st.spinner("Chargement des données..."):
    all_business = query_db("SELECT name, business_id FROM business_table;")
    if (len(all_business) > 0):
        st.markdown("---")
        st.markdown("### 🏢 Infos sur les entreprises")
        business_options = {row['name']: row['business_id'] for _, row in all_business.iterrows()}
        selected_name = st.selectbox("Sélectionner une entreprise...", list(business_options.keys()))
        selected_business_id = business_options[selected_name]
        business = query_db(f"SELECT * FROM business_table WHERE business_id = '{selected_business_id}'")
        st.write(business)
        all_business_review = query_db(f"SELECT * FROM review_table WHERE business_id = '{selected_business_id}'")
        st.write(all_business_review)