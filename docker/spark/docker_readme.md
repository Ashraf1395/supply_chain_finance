```markdown
# Running Spark and Kafka Clusters on Docker

## 1. Build Required Images for Running Spark

To build the required Spark images, refer to the blog post written by André Perez on Medium blog - Towards Data Science for detailed instructions on how the Spark images are created in different layers.

```bash
# Build Spark Images
./build.sh
```

## 2. Create Docker Network & Volume

```bash
# Create Network
docker network create kafka-spark-network

# Create Volume
docker volume create --name=hadoop-distributed-file-system
```

## 3. Run Services on Docker

```bash
# Stop Docker-Compose (Run in the root directory)
docker-compose -f ./docker/spark/docker-compose.yml up -d 
```
We don't need to run docker all the commands to run the project are in start.sh we just need to run it.

## 4. Stop Services on Docker

```bash
# Stop Docker-Compose
docker-compose down
```

## 5. Helpful Commands

```bash
# Delete all Containers
docker rm -f $(docker ps -a -q)

# Delete all volumes
docker volume rm $(docker volume ls -q)
```
