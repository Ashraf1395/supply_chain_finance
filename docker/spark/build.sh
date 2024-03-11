#!/bin/bash

# Remove carriage return characters from the script
sed -i 's/\r//' "$0"

# -- Software Stack Version
SPARK_VERSION="3.3.1"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="3.6.1"
CURRENT_PATH="./docker/spark/"

# -- Building the Images

docker build \
  -f "${CURRENT_PATH}cluster-base.Dockerfile" \
  -t cluster-base .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f "${CURRENT_PATH}spark-base.Dockerfile" \
  -t spark-base .

docker build \
  -f "${CURRENT_PATH}spark-master.Dockerfile" \
  -t spark-master .

docker build \
  -f "${CURRENT_PATH}spark-worker.Dockerfile" \
  -t spark-worker .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f "${CURRENT_PATH}jupyterlab.Dockerfile" \
  -t jupyterlab .
