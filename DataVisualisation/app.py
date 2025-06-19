from streamlit_autorefresh import st_autorefresh
from src.database.getDataFromDatabase import *
import streamlit as st
import plotly.express as px
import re
from wordcloud import STOPWORDS, WordCloud
import wordcloud
import numpy as np
import matplotlib.pyplot as plt

st_autorefresh(interval=1000)

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

#########################   VISUALISATIONS   #######################
st.title("📝 Dashboard Yelp")

# -----------------------   Stats générales   ----------------------
st.markdown("---")
st.markdown("### 📌 Statistiques Générales")

review_evolution = query_db("SELECT * FROM review_evolution_table;")

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

# ----------------------- Review Evolution dans le temps ----------------------
st.markdown("---")
st.markdown("### 📈 Nombre de reviews journaliers")
st.markdown("Cette courbe montre l'évolution quotidienne du volume de reviews enregistrées.")

review_evolution["parsed_date"] = pd.to_datetime(review_evolution["parsed_date"])

fig = px.line(
    review_evolution,
    x="parsed_date",
    y="total_reviews",
    labels={"parsed_date": "Date", "total_reviews": "Nombre de reviews"},
    title="Évolution des reviews dans le temps"
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Nombre de reviews",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

review_evolution["cumulative_reviews"] = review_evolution["total_reviews"].cumsum()

st.markdown("### 📊 Nombre total de reviews cumulées")
st.markdown("Ce graphique montre la croissance cumulative du nombre de reviews au fil du temps.")

fig_cum = px.line(
    review_evolution,
    x="parsed_date",
    y="cumulative_reviews",
    labels={"parsed_date": "Date", "cumulative_reviews": "Total cumulatif de reviews"},
    title="Croissance cumulative des reviews"
)

fig_cum.update_layout(
    xaxis_title="Date",
    yaxis_title="Total cumulatif de reviews",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig_cum, use_container_width=True)

# ----------------------- Catégories les plus fréquentes ----------------------
with st.spinner("chargement de données ..."):
    top_categories = query_db("SELECT * FROM top_categories_table;")

st.markdown("---")
st.markdown("### 🥧 Analyse des catégories les plus fréquentes")
if top_categories.empty:
    st.warning("Aucune donnée disponible dans la table `top_categories_table`.")
else:
    fig = px.pie(top_categories, names='category', values='count')
    st.plotly_chart(fig)

# ----------------------- Classement des entreprises ----------------------
st.markdown("---")
st.markdown("### 🔍 Classement des entreprises selon les avis clients")

with st.spinner("Chargement des données..."):
    # top_rated_business = query_db("SELECT name, average_stars FROM business_table ORDER BY average_stars DESC LIMIT 10")
    total_review_business = query_db("SELECT name, total_reviews FROM business_table ORDER BY total_reviews DESC LIMIT 10")
    top_fun_business = query_db("SELECT name, funny_count FROM business_table ORDER BY funny_count DESC LIMIT 10")
    top_useful_business = query_db("SELECT name, useful_count FROM business_table ORDER BY useful_count DESC LIMIT 10")

if total_review_business.empty or top_fun_business.empty or top_useful_business.empty:
    st.warning("Aucune donnée disponible dans la table `business_table`.")
else:
    col1, col2, col3 = st.columns(3)

    def colorize(df, column, cmap="Blues"):
        styled = df.style.background_gradient(subset=[column], cmap=cmap)
        return styled

    with col1:
        st.markdown("#### 🏆 Top 10 - Les plus notes")
        df1 = total_review_business.rename(columns={"total_reviews": "Nombres de Note", "name": "Entreprise"})
        df1 = df1[["Nombres de Note", "Entreprise"]]
        st.dataframe(colorize(df1, "Nombres de Note", "Greens"))

    with col2:
        st.markdown("#### 😂 Top 10 - Les plus drôles")
        df2 = top_fun_business.rename(columns={"funny_count": "Votes Funny", "name": "Entreprise"})
        df2 = df2[["Votes Funny", "Entreprise"]]
        st.dataframe(colorize(df2, "Votes Funny", "Oranges"))

    with col3:
        st.markdown("#### 👍 Top 10 - Les plus utiles")
        df3 = top_useful_business.rename(columns={"useful_count": "Votes Useful", "name": "Entreprise"})
        df3 = df3[["Votes Useful", "Entreprise"]]
        st.dataframe(colorize(df3, "Votes Useful", "Purples"))

st.markdown("---")
st.markdown("### 🏢 Infos sur les entreprises")
with st.spinner("Chargement des données..."):
    business_df = query_db("SELECT * FROM business_table ORDER BY avg_stars DESC")

if business_df.empty:
    st.warning("Aucune donnée disponible dans la table `business_table`.")
else:
    # Création des options pour le selectbox
    business_options = {row['name']: row for _, row in business_df.iterrows()}
    selected_name = st.selectbox("Sélectionner une entreprise...", list(business_options))
    selected_business = business_options[selected_name]

    # Affichage de l’entreprise sélectionnée
    st.markdown("#### 🧾 Détails de l'entreprise sélectionnée")
    categories = selected_business.get("categories", "")
    if categories:
        st.markdown("**Catégories :**")
        cat_list = [cat.strip() for cat in categories.split(",") if cat.strip()]
        # Affichage stylisé avec des spans en ligne
        cat_html = "".join([
            f"<span style='display:inline-block; background-color:#eee; color:#333; padding:4px 10px; \
            margin:4px 4px 4px 0; border-radius:20px; font-size:0.85rem;'>{cat}</span>"
            for cat in cat_list
        ])
    
        st.markdown(cat_html, unsafe_allow_html=True)

    # Requête des avis
    review_df = query_db(f"SELECT text FROM review_table WHERE business_id = '{selected_business['business_id']}'")

    if review_df.empty:
        st.info("Aucun avis disponible pour cette entreprise.")
    else:
        all_text = " ".join(review_df['text'].dropna().tolist()).lower()
        all_text = re.sub(r"[^a-zA-Z\s]", "", all_text)

        wordcloud = WordCloud(
            width=800, height=400,
            background_color='white',
            stopwords=STOPWORDS,
            max_words=100,
            max_font_size=90,
            random_state=42
        ).generate(all_text)

        st.markdown(f"### Nuage de mots pour **{selected_business['name']}**")
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)
