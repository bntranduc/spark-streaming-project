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
