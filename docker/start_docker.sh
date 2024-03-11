# Create Docker network and volume
docker network create "${PROJECT_NAME}-network"
docker volume create --name=hadoop-distributed-file-system

# Start containers
start_containers() {
    echo "Starting $1 related containers"
    docker-compose -f "$2" up -d
    echo
}

start_containers "Postgres" "./docker/postgres/docker-compose.yml"
start_containers "Mage" "./docker/mage/docker-compose.yml"
start_containers "Metabase" "./docker/metabase/docker-compose.yml"
start_containers "Kafka" "./docker/kafka/docker-compose.yml"

echo "Starting Spark related containers"
chmod +x ./docker/spark/build.sh
./docker/spark/build.sh
echo
docker-compose -f ./docker/spark/docker-compose.yml up -d
echo

echo "Started all containers. You can check their status with 'docker ps'."