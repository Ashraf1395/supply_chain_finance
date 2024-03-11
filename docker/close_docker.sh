# Function to stop containers
stop_containers() {
    echo "Closing $1 related containers"
    docker-compose -f "$2" down
    echo
}

# Stop containers
stop_containers "Postgres" "./docker/postgres/docker-compose.yml"
stop_containers "Mage" "./docker/mage/docker-compose.yml"
stop_containers "Metabase" "./docker/metabase/docker-compose.yml"
stop_containers "Kafka" "./docker/kafka/docker-compose.yml"
stop_containers "Spark" "./docker/spark/docker-compose.yml"

echo "Closed all containers."