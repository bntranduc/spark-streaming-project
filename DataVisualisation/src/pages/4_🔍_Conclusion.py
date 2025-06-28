import streamlit as st

st.set_page_config(
    page_title="Yelp Dashboard – Analyse des avis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("# Conclusion du Projet Yelp Analytics")
st.markdown("---")

st.markdown("##  Ce que nous avons accompli")

st.markdown("""
### Une analyse multidimensionnelle des avis négatifs
Nous avons mené une investigation complète pour comprendre les **causes profondes** des mauvaises évaluations sur Yelp à travers :

- **L'analyse des utilisateurs** : Profilage des reviewers sévères et détection des comportements extrêmes
- **L'étude des entreprises** : Identification des catégories et localisations à risque
- **L'examen des avis** : Saisonnalité, mots-clés récurrents et utilité perçue
""")

st.markdown("## Principaux enseignements")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ###  Du côté des utilisateurs
    - Une minorité d'utilisateurs génère une part disproportionnée d'avis négatifs
    - Les "serial offenders" ciblant plusieurs établissements existent mais sont rares
    - Les avis polarisés (1★ ou 5★) révèlent des biais émotionnels forts
    """)

with col2:
    st.markdown("""
    ### Du côté des entreprises
    - Certaines catégories (fast-food, services urgents) sont plus exposées
    - Des variations géographiques significatives apparaissent
    - Les établissements fermés avaient en moyenne des notes plus basses
    """)

st.markdown("""
## Applications concrètes
Ces analyses permettent de :

- **Pour Yelp** :  
  ✓ Détecter les avis suspects ou biaisés  
  ✓ Améliorer l'expérience utilisateur  

- **Pour les entreprises** :  
  ✓ Identifier leurs points faibles récurrents  
  ✓ Adapter leur service aux périodes critiques  

- **Pour les consommateurs** :  
  ✓ Mieux interpréter les notes extrêmes  
  ✓ Se focaliser sur les avis les plus utiles  
""")

st.markdown("""
## Perspectives d'amélioration
Ce projet pourrait être enrichi par :

- Une analyse de sentiment plus poussée sur le texte des avis
- L'intégration de données externes (météo, événements locaux)
- Une modélisation prédictive du risque de mauvaise note
""")

st.markdown("---")
st.success("""
**En conclusion**, cette analyse fournit des insights actionnables pour toutes les parties prenantes 
de l'écosystème Yelp, tout en démontrant la puissance des données pour comprendre 
les comportements consommateurs.
""")