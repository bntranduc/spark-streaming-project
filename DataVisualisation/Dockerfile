# Utiliser une image officielle de Python
FROM python:3.11-slim

# Définir le dossier de travail
WORKDIR /app

# Copier les fichiers
COPY app.py ./

# Installer Streamlit
RUN pip install streamlit

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Commande pour lancer l'application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
