PROJECT_NAME='ashraf-magic'
EXPORT_TO_BIGQUERY_PIPELINE_UUID='94ab2c7a2aa24bde8e148ef84c88a10f'
# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    echo "Network ${PROJECT_NAME}-network already exists."
fi

# Function to start streaming data
stream-data() {
	docker-compose -f ./docker/streaming/docker-compose.yaml up
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
	docker-compose -f ./docker/spark/docker-compose.yml up -d
}

# Function to start Mage
start-mage() {
   docker-compose -f ./docker/mage/docker-compose.yml up -d
   sleep 5
   sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/kafka_to_gcs.yaml ./docker/mage/${PROJECT_NAME}/data_exporters/
   sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/consume_from_kafka.yaml ./docker/mage/${PROJECT_NAME}/data_loaders/
   sudo mkdir ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming
   sudo cp ./streaming_pipeline/kafka_to_gcs_streaming/metadata.yaml ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/
   sudo touch ./docker/mage/${PROJECT_NAME}/pipelines/kafka_to_gcs_streaming/__init__.py

   sudo cp ./batch_pipeline/export_to_big_query/data_exporters/* ./docker/mage/${PROJECT_NAME}/data_exporters/
   sudo cp ./batch_pipeline/export_to_big_query/data_loaders/* ./docker/mage/${PROJECT_NAME}/data_loaders/
   sudo mkdir ./docker/mage/${PROJECT_NAME}/pipelines/export_to_big_query
   sudo cp ./batch_pipeline/export_to_big_query/*.yaml ./docker/mage/${PROJECT_NAME}/pipelines/export_to_big_query/
   sudo touch ./docker/mage/${PROJECT_NAME}/pipelines/export_to_big_query/__init__.py
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
    sleep 5
    start-mage
    stream-data
}

# Function to stop the streaming pipeline
stop-streaming-pipeline(){
    # Stop Kafka and Mage
    stop-kafka
    stop-mage
}

olap-transformation-pipeline(){
    # Execute the Python batch pipeline script
    python batch_pipeline/export_to_gcs/pipeline.py
}

gcs-to-bigquery-pipeline(){
    curl -X POST http://127.0.0.1:6789/api/pipeline_schedules/2/pipeline_runs/f0607c7c9c0241208bf779edfe0c5f9d \
  --header 'Content-Type: application/json' \
  --data '
    {
    "pipeline_run": {
        "variables": {
        "key1": "value1",
        "key2": "value2"
        }
    }
    }'
}

start-batch-pipeline(){
    olap-transformation-pipeline
    gcs-to-bigquery-pipeline
}



gitting(){
    git add .
    sleep 2
    git commit -m "Update from Local"
    sleep 2
    git push -u origin main
}

terraform-start(){
    terraform -chdir=terraform init
    terraform -chdir=terraform plan
    terraform -chdir=terraform apply
}
terraform-destroy(){
    terraform -chdir=terraform destroy
}



start-project(){
    echo "Creating Resources in Bigquery..."
    sleep 3
    terraform-start
    echo "Resources created, starting the streaming pipeline..."
    start-streaming-pipeline
    echo "Execute the mage_kafka_to_gcs pipeline from mage ui"
    sleep 10
    echo "Waiting 2 mins to get some data, till then update the api endpoint for batch pipeline."
    sleep 120
    echo "Starting Batch pipeline..."
    start-spark
    start-batch-pipeline
    sleep 30
    echo "Batch pipeline execution complete,starting dbt pipeline..."
    dbt run
    echo "dbt pipeline execution complete, your data is ready in Bigquery for downstream usecases."
    echo "Start making dashboard in metabase"
    start-metabase
}


stop-all-services(){
    stop-mage
    stop-kafka
    stop-spark
    stop-metabase
}