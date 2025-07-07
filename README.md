# spark-streaming-project

## Installer java-11
```
sudo apt update
sudo apt install openjdk-11-jdk
```

Sélectionner la version 11 de Java à exécuter :
```
sudo update-alternatives --config java
sudo update-alternatives --config javac
sdk install java 11.0.20-tem
```

Installer sbt :
```
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install sbt
sbt sbtVersion
```

## Installer postgresql
```
sudo apt install postgresql postgresql-contrib
```

## Se connecter a la bdd de docker
```
sudo -u postgres psql
```
ou
```
psql -h localhost -p 5432 -U user -d mydatabase
```
Copier-coller les instructions dans init/init.sql

## Clear le cache
```
cd Producer
sbt clean assembly

cd ../Consumer
sbt clean assembly
```

## Lancer tout le projet
```
docker-compose up --build
```

## Arrêter l'environnement
```
docker-compose down
```

## Voir les logs
```
docker-compose logs -f
```

docker-compose down --volumes --remove-orphans
docker system prune -a --volumes

## Commande Docker Compose complète pour tout réinitialiser 
docker compose down --volumes --remove-orphans --rmi all
