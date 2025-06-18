# spark-streaming-project

# Lancer tout le projet
docker-compose up --build

# Arrêter
docker-compose down

# Voir les logs
docker-compose logs -f

# Clear le cache
cd Producer
sbt clean

cd ../Consumer
sbt clean

docker-compose down --volumes --remove-orphans
docker system prune -a --volumes

# change java version and select 11
sudo update-alternatives --config java

## Se connecter a la bdd de docker
psql -h localhost -p 5432 -U user -d mydatabase

# Commande Docker Compose complète pour tout réinitialiser 
docker compose down --volumes --remove-orphans --rmi all

**Liste d’analyses finales à présenter sur l’application :**

1. __Les entreprises les plus notés et les mieux notés par catégories__
    1. categories, name (business)
    2. count of review_id & avg of stars (review)

2. __Les établissements les plus populaires par mois__
    1. count of review_id by business_id by date (review)
    
3. __Les établissements les plus cool / fun / useful__
    1. count of useful by business_id (review)

4. __Taux de fermeture des établissements selon leur note__
    1. avg of stars & count of review_id if business.is_open = false (business)

5. __Evolution de l’activité sur Yelp par mois/an__
    1. count of review_id by date (review)
    2. count of user_id by date (user)
    3. count of business_id by date ?? (business)
    
6. __Les utilisateurs les plus influents (utiles / fun / cool)__
    1. count of useful by user_id (user)

7. __Les utilisateurs les plus populaires__
    1. count fans by user_id & count friends by user_id (user)

8. __Les utilisateurs les plus fidèles par entreprise__
    1. count pair (review_id, business_id) (review)

9. __Apex Predator users__
    1. user_id with max elite (user)
    
10. __Impact du statut sur les notes__
    1. avg stars for users with elite exist vs without elite