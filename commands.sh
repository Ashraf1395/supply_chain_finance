PROJECT_NAME='ashraf-magic'

# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    echo "Network ${PROJECT_NAME}-network already exists."
fi

# Function to start streaming data
stream-data() {
	docker-compose -f ./docker/streaming/docker-compose.yml up
}

# Function to start Kafka
start-kafka() {
	docker-compose -f ./docker/kafka/docker-compose.yml up -d
}

# Function to start Spark
start-spark() {
    # Ensure the build script is executable and run it
    chmod +x ./docker/spark/build.sh
    ./docker/spark/build.sh
	# Start Spark containers
	docker-compose -f ./docker/spark/docker-compose.yml up
}

# Function to start Mage
start-mage() {
   docker-compose -f ./docker/mage/docker-compose.yml up -d
}

# Function to start Postgres
start-postgres() {
   docker-compose -f ./docker/postgres/docker-compose.yml up -d
}

# Function to start Metabase
start-metabase() {
   docker-compose -f ./docker/metabase/docker-compose.yml up -d
}

# Function to stop Kafka
stop-kafka() {
    docker-compose -f ./docker/kafka/docker-compose.yml down
}

# Function to stop Spark
stop-spark() {
    docker-compose -f ./docker/spark/docker-compose.yml down
}

# Function to stop Mage
stop-mage() {
    docker-compose -f ./docker/mage/docker-compose.yml down
}

# Function to stop Postgres
stop-postgres() {
    docker-compose -f ./docker/postgres/docker-compose.yml down
}

# Function to stop Metabase
stop-metabase() {
    docker-compose -f ./docker/metabase/docker-compose.yml down
}

# Function to start the streaming pipeline
start-streaming-pipeline(){
    # Start Kafka and Mage, then begin streaming data
    start-kafka
    start-mage
    stream-data
}

# Function to stop the streaming pipeline
stop-streaming-pipeline(){
    # Stop Kafka and Mage
    stop-kafka
    stop-mage
}

# Function to start the batch pipeline
start-batch-pipeline(){
    # Execute the Python batch pipeline script
    python batch_pipeline/pipeline.py
}


#Committing to Github

git() {
    chmod +x ./git.sh
    ./git.sh
}