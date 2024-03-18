# Dockerized Project Overview

Docker serves as the backbone of this project, providing a containerized environment for running various applications seamlessly. Each application is encapsulated within its own Docker container, ensuring isolation, portability, and scalability. Below are the key components of the Dockerized project, organized into separate folders:

## Kafka

The [Kafka folder](./kafka/) contains services related to Apache Kafka, a distributed streaming platform:

- **kafka_broker**: Apache Kafka broker service responsible for handling message storage and replication.
  
- **kafka-rest**: Kafka REST Proxy service for interacting with Kafka using HTTP RESTful APIs.

- **control-center**: Confluent Control Center service for monitoring and managing Kafka clusters.

- **zookeeper**: Apache ZooKeeper service, used for coordination and synchronization tasks within Kafka clusters.

- **schema_registry**: Schema Registry service for managing Avro schemas used in Kafka topics.

## Mage

The [Mage folder](./mage/) contains services related to the Mage platform, facilitating the execution of data pipelines:

- **mage**: Mage service responsible for running all pipelines and orchestrating data processing tasks.

## Postgres

The [Postgres folder](./postgres/) contains services related to the PostgreSQL database management system:

- **postgres-server**: PostgreSQL server service, providing a relational database management system for storing structured data.

- **pgadmin**: PgAdmin service, offering a web-based interface for managing PostgreSQL databases.

## Metabase

The [Metabse folder](./metabase/) contains services related to the Metabase dashboarding tool:

- **metabase**: Metabase service for creating and sharing data visualizations and dashboards.

## Spark

The [Spark folder](./spark/) contains services related to Apache Spark, a distributed computing framework:

- **spark-master**: Apache Spark master service responsible for coordinating Spark applications.

- **spark-workers**: Apache Spark worker services for executing tasks within Spark applications.

- **jupyter-notebook**: Jupyter Notebook service for interactive data analysis and visualization using Python and Spark.

## Streaming

The [Streaming folder](./streaming/) contains services for producing messages to be sent to Kafka:

- **local-service**: Local service for producing messages, simulating real-time data streams to Kafka topics.

All of these services are interconnected and communicate with each other within a single Docker network. Docker containers offer a lightweight and efficient way to deploy and manage the project's components, ensuring consistency and reliability across different environments. By leveraging Docker, the project benefits from increased flexibility, scalability, and ease of deployment, making it well-suited for modern data processing and analytics workflows.