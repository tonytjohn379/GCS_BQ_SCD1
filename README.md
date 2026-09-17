This project demonstrates a batch data processing pipeline that transfers product data from Google Cloud Storage (GCS) to Google BigQuery using Apache Beam and Google Cloud Dataflow.

The pipeline follows an ETL (Extract, Transform, Load) approach:

CSV Files
   -
Google Cloud Storage (GCS)
   -
Google Cloud Dataflow
   -
Data Transformation
   -
BigQuery Staging Table
   -
SCD Type 1 MERGE
   -
BigQuery Main Table

The project implements Slowly Changing Dimension Type 1 (SCD Type 1) to maintain the latest version of product information. When an existing product is received in a new batch, its old information is overwritten with the latest information.
