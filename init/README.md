# Commands de bases

| Commande      | Description                                        |
| ------------- | -------------------------------------------------- |
| `\l`          | Lister toutes les bases de données                 |
| `\c <dbname>` | Se connecter à une autre base de données           |
| `\dt`         | Lister toutes les tables dans la base courante     |
| `\d <table>`  | Afficher la structure d'une table                  |
| `\q`          | Quitter le client `psql`                           |
| `\du`         | Lister les rôles (utilisateurs PostgreSQL)         |
| `\?`          | Lister toutes les commandes internes `psql`        |
| `\h`          | Aide sur une commande SQL (ex : `\h CREATE TABLE`) |


# Connect
sudo -u postgres psql

# Creer une bd
CREATE DATABASE spark_streaming_db;

# Supprimer une bd
DROP DATABASE spark_streaming_db;

# Creer un user
CREATE USER DivinAndreTomAdam WITH PASSWORD 'oDAnmvidrTnmeiAa';
ALTER ROLE DivinAndreTomAdam WITH SUPERUSER CREATEDB CREATEROLE LOGIN;

# Executer un script sql
psql -U postgres -d spark_streaming_db -f init.sql

# se connect a db
psql -U divinandretomadam -h localhost -d spark_streaming_db
