Bien sûr ! Voici une version plus claire, structurée et professionnelle de ton README pour ton projet **Spark Streaming** :

---

# Spark Streaming - Guide d'installation et de test

## Prérequis

* **Git**
* **Docker & Docker Compose**
* **SBT (Scala Build Tool)**
* **Java JDK 11**

---

## Installation

1. **Cloner le dépôt**

```bash
git clone git@github.com:bntranduc/spark-streaming-project.git &&
cd spark-streaming-project
```

2. **Préparer les données**

Dézippez le dataset dans le répertoire principal du projet :

```bash
unzip yelp_dataset.zip
```

3. **Configurer les variables d'environnement**

Copiez le fichier d'exemple `.env.exemple` en `.env` :

```bash
mv .env.exemple .env
```

4. **Compiler les modules Scala**

### Consumer

```bash
cd Consumer
sbt clean assembly
```

### Producer

```bash
cd ../Producer
sbt clean assembly
```

5. **Lancer les services avec Docker**

Assurez-vous d’être dans le répertoire principal du projet, puis exécutez :

```bash
docker compose up --build
```

---

## Accès aux interfaces de monitoring

| Service                 | URL                                     | Description                          |
| ----------------------- | --------------------------------------- | ------------------------------------ |
| **Dashboard Streamlit** | [localhost:8501](http://localhost:8501) | Visualisation des flux en temps réel |
| **Spark UI - Producer** | [localhost:4040](http://localhost:4040) | Interface Spark du Producer          |
| **Spark UI - Consumer** | [localhost:4041](http://localhost:4041) | Interface Spark du Consumer          |
