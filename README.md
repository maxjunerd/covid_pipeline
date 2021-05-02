# COVID-19 Situation in Thailand Pipeline

The data in this pipeline is the COVID-19 situation in Thailand from [the Department Disease Control API](https://covid19.th-stat.com/th/api). The pipeline consists of 2 tasks. 

![](https://github.com/maxjunerd/covid_pipeline/blob/main/pic/dag.jpg?raw=true)

The first task is to download data from API and save it to Google Cloud Storage. The second task is to ingest data from Google Cloud Storage to Google BigQuery. This pipeline runs on Google Cloud Composer which is a managed service for data workflow orchestration. Google Cloud Composer is built on Apache Airflow. Google Data Studio is used to create a dashboard to visualize data.

![](https://github.com/maxjunerd/covid_pipeline/blob/main/pic/dashboard.jpg?raw=true)

