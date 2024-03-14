FROM python:3.11

RUN apt-get update && apt-get install -y --no-install-recommends \
  wget \
  software-properties-common \
  lsb-release \
  gcc \
  make \
  libsasl2-modules-gssapi-mit \
  krb5-user\
  && wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add - \
  && add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" \
  && apt update \
  && apt install -y librdkafka-dev

COPY . /home/streaming_pipeline
# COPY ../data /home/streaming_pipeline

RUN pip3 install confluent-kafka pyarrow