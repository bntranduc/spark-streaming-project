from database.getDataFromDatabase import *
import streamlit as st

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# 👤 **Utilisateurs**")
st.sidebar.header("👤 **Utilisateurs**")

st.write("""
Bienvenue sur le **Yelp Dashboard**, une application interactive conçue pour explorer les **raisons derrière les mauvaises notes** données aux entreprises sur Yelp.  
""")
