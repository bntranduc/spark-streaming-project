from streamlit_autorefresh import st_autorefresh
from src.getDataFromDatabase import *
import streamlit as st

st_autorefresh(interval=2000, limit=100)


st.title("📝 Dashboard Yelp")

# ---------- VISUALISATIONS ----------

# Interface Streamlit
col1, col2, col3 = st.columns(3)
#1 user nbr

col1.write("Nombre d'utilisateur :")
all_user = query_db("SELECT COUNT(*) FROM user_table;")
col1.write(all_user['count'][0])

#2 review nbr
col2.write("Nombre de review :")
all_review = query_db("SELECT COUNT(*) FROM review_table;")
col2.write(all_review['count'][0])

#3 business nbr
col3.write("Nombre d'entreprise :")
all_query_business = query_db("SELECT COUNT(*) FROM business_table;")
col3.write(all_query_business['count'][0])
