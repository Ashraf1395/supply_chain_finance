For streaming_pipeline

Dockerfile should be inside the streaming_pipeline and data folder as well
The current repository in the dockerfile is the repository where the dockerfile is present
for example here current directory for stream dockerfile is streaming_pipeline

For docker-compose.yaml file in the context path we have to specifiy where your dockerfile is present 
and current directory for the docker-compose.yaml is also the reposoitory where it resides 
for example here the current_directory for docker-compose.yaml file is docker/stream/

And the path for dockerfile from docker-compose.yaml file is
../../streaming_pipeline

In config.py the boostrap server will be the advertised kafka listener i.e kafka:29092
In the mage data_loader block for streaming pipeline
the boostreap server will be kafka:29092

First run kafka then mage then postgres then stream