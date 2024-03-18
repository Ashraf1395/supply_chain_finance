## Debugging Guide

To ensure smooth execution of the streaming pipeline and associated components, follow these guidelines:

## Dockerfile and Docker Compose Configuration

When setting up the Dockerfile and Docker Compose configuration for the streaming pipeline, follow these guidelines:

1. **Dockerfile Placement**

    - **Location:** Place the Dockerfile inside the `streaming_pipeline` folder.
    - **Repository Context:** The current directory for the Dockerfile is the repository where it is present. For instance, in the streaming pipeline, the current directory for the Dockerfile is `streaming_pipeline`.

2. **Docker Compose Configuration**

    - **Context Path:** In the `docker-compose.yaml` file, specify the context path where the Dockerfile is located.
    - **Current Directory:** The current directory for the `docker-compose.yaml` file is also the repository where it resides. For example, in the streaming pipeline, the current directory for the `docker-compose.yaml` file is `docker/stream/`.
    - **Path to Dockerfile:** From the `docker-compose.yaml` file, specify the path to the Dockerfile as `../../streaming_pipeline`.

3. **Kafka and Mage Configuration:**
   - Update the `bootstrap_server` in the `config.py` and Mage's `data_loader` block to `kafka:29092`, which is the advertised Kafka listener.

4. **Execution Order:**
   - Ensure Kafka, Mage, PostgreSQL, and the stream are started in the correct sequence. First run Kafka, followed by Mage, then PostgreSQL, and finally the stream.

5. **DBT Directory Configuration:**
   - Set the directories for DBT (models, macros, analyses, tests) inside a `business_transformation` folder.
   - Update the paths in the `dbt_project.yaml` accordingly.

6. **Environment Variables:**
   - If encountering the error "Project_name variable not set," update the `project_name` variable in the `.env` file of the respective application.
   - For example, update the `project_name` variable in the `.env` file of Metabase to resolve any errors related to it.

7. **Column Name Restrictions in DBT:**
   - Avoid using `()` in column names in DBT. Replace any occurrences of `()` in column names, such as `days_of_shippment_real(Date Orders)` and `days_of_shippment_scheduled(Date Orders)`.


By following these debugging guidelines, you can ensure smooth execution of the streaming pipeline and associated components, mitigating any errors or issues encountered during the process.