
## Overview
The aim of this project is to compare the time it takes to extract, transform and load large datasets using three different methods.
Please read the [Medium post](https://medium.com/@cjmcguicken/working-with-large-datasets-bigquery-with-dbt-vs-spark-vs-dask-92e596ce8e06) for more information.

## Data
The data used in this project is the publicly available [Stack Exchange Data Dump](https://archive.org/details/stackexchange). `Users.xml` and `Posts.xml` were converted to `users.csv.gz` and `posts.csv.gz` and used as the source files for this project.

For the ISO 3166-1 country codes, the CSV used from [DataHub](https://datahub.io/core/country-list) was used (as `country_codes.csv`).

## Pre-requisites
For Google Cloud:
- Create a project
- Create a Cloud Storage Bucket and upload `posts.csv.gz`, `users.csv.gz` and `country_codes.csv` files 

For Scala:
- [Install sbt](https://www.scala-sbt.org/1.x/docs/Setup.html)
- Install [SDKMAN!](https://sdkman.io/)
- In the `spark` folder, use SDKMAN! to install JDK (8 or 11 currently supported) and set JDK version of the project using `sdk env init`

For Dask:
- Python 3.x installed
- Install packages from `requirements.txt` -> `pip install -r /dask/requirements.txt` (for running on local)

For dbt:
- Python 3.x installed
- Install packages from `requirements.txt` -> `pip install -r /dbt/requirements.txt`
- Copy the ISO 3166-1 country codes CSV into `./bigquery_dbt/seeds/country_codes.csv`
- Setup a dbt profile in `~/.dbt/profiles.yml` called `bigquery_dbt` for BigQuery ([Example](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile))
## Running
### BigQuery (with dbt)
1. Make BigQuery dataset
`bq mk --dataset ${PROJECT_ID}:${DATASET}`

2. Load files into BigQuery as tables (can be done concurrently)
```
bq load \
    --autodetect \
    --source_format=CSV \
    ${DATASET}.posts \
    gs://${BUCKET_NAME}/posts.csv.gz

bq load \
    --autodetect \
    --source_format=CSV \
    ${DATASET}.users \
    gs://${BUCKET_NAME}/users.csv.gz
```

3. Ensure Google project id is specified in `database` field in [`schema.yml`](bigquery_dbt/models/schema.yml)

4. Run dbt
```
cd ./bigquery_dbt
dbt build # Load CSV as reference table (via seeds), run tests etc.
dbt run
```

5. Load created table into GCS
```
bq extract \
--destination_format CSV \
--compression GZIP \
--field_delimiter ',' \
${PROJECT_ID}:${DATASET}.aggregated_users \
gs://${BUCKET_NAME}/dbt_bigquery/agg_users.csv.gz
```

### Spark
1. Ensure that you change the `gcsBucket` value in [`aggregate-users.scala`](spark/aggregate-users.scala)
2. Run the following (in the `spark` folder) to compile and package the project into a `.jar` for Dataproc:
```
sbt
```
Then within the sbt console:
```
package
```

3. Copy the `jar` from local to GCS (optional):
```
gsutil cp spark/target/scala-2.12/${JAR_FILENAME}.jar gs://example-big-data-bucket/spark/aggregateusers.jar
```

4. Create Dataproc cluster:
```
 gcloud dataproc clusters create ${SPARK_CLUSTER_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --image-version=2.0 \
    --master-machine-type n1-standard-8 \
    --worker-machine-type n1-standard-8 \
    --num-workers 6
```

5. Submit Spark job on Dataproc cluster
```
gcloud dataproc jobs submit spark \
    --cluster=${SPARK_CLUSTER_NAME} \
    --class=stackoverflow.AggregateUsers \
    --jars=gs://${BUCKET_NAME}/spark/aggregateusers.jar \
    --region=${REGION}
```

6. Delete cluster when finished

### Dask
1. Copy initialisation actions to local bucket (optional):
```
gsutil cp gs://goog-dataproc-initialization-actions-${ZONE}/dask/dask.sh gs://${BUCKET_NAME}/dask/
```

2. Create cluster
```
 gcloud dataproc clusters create ${DASK_CLUSTER_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --master-machine-type n1-standard-8 \
    --worker-machine-type n1-standard-8 \
    --num-workers 6 \
    --image-version preview-ubuntu \
    --initialization-actions gs://${BUCKET_NAME}/dask/dask.sh \
    --metadata dask-runtime=yarn \
    --enable-component-gateway
```

3. Copy files
```
gcloud compute scp \
    --project=${PROJECT_ID} \
    --zone=${ZONE} \
    --recurse ./dask/ ${DASK_CLUSTER_NAME}-m:~/
```

4. Install package requirements & run
```
gcloud compute ssh ${CLUSTER_NAME}-m --zone ${ZONE}
/opt/conda/default/bin/python -m pip install python-dotenv
/opt/conda/default/bin/python ./dask/transform.py
```

5. Delete cluster when finished

