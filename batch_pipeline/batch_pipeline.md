The Batch Pipeline is divided into two parts:
- Firslty : A Spark Pipeline to Take the raw data from GCS and Do necessary olap transformations on it and store it in GCS as a transformed(Silver level) data.

- Second: A Mage Pipeline to Take the transformeed(Silver level)data from GCS and send it to bigquery.


Spark Pipeline: Transform and Export Raw Data to GCS as Silver Level Data
olap-transformation-pipeline after starting spark with - start-spark command

Architecture of this Pipeline

Database besign of thie transformed data

Summary


Mage Pipeline: Export Transformed silver level data from gcs into bigquery
To run this pipeline Run this coomand: gcs-to-bigquery-pipeline
This pipeline send an api request to mage to run this pipeline

Update the api credentials of the pipeline correctly in the source file
Here's an example

This is the Block level overview of the Pipeline.



