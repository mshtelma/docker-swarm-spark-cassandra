# Ingesting JSON data to Cassandra using Spark SQL

This project illustrates how simple json dataset (using Yelp dataset as an example) can be ingested in Cassandra using Spark SQL. 
The whole environment can be deployed using Docker Swarm. 


## Getting started

The project can be run using the following steps:

- Initialize Docker Swarm using 
`docker swarm init`
- Start stack including Cassandra node, Spark SQL cluster with three workers
`sh start-stack.sh `
- Create a service that will run the actual pipeline (the script accepts path to tar.gz archive with the Yelp dataset)
`sh start-job.sh ~/Downloads/yelp_dataset.tar.gz `

The scripts are going to save temporary files in /tmp filesystem, so this means, that the read/write permissions for the tmp filesyste are required. 


## Architecture

The whole solution can be run using Docker Swarm. There are  following services:
- Cassandra node. It uses existing docker container for Cassandra
- Spark Master - Master node for Spark Standalone cluster.
- Spark Worker - Worker nodes for Spark. It is configured 3 replicas of workers in the cluster.

Spark services are using docker container based on Alpine Linux with OpenJDK.
The Dockerfile is included in this repository: https://github.com/mshtelma/docker-swarm-spark-cassandra/blob/master/containers/spark-base/Dockerfile

The configuration of Spark master and workers can be changed in docker-compose.yml.

The actual pipeline can be started using script start-jobs.sh, which builds the jar and docker container with the jar.
Afterwards this newly built docker container will be started as docker service, which will run the pipeline on spark cluster.

The Spark implementation of the pipeline consists of the main class IngestPipelineMain, where the main logic was implemented.
The pipeline implements full load scenario, so that the existing tables in cassandra are recreated and populated with the new data.
The files are loaded in following order: at first two parent entities (user and business) are loaded.
Afterwards all other dependent entities are loaded. During the load the foreign keys of the child entities are being checked against already loaded parents.
After each entity was loaded, the number of loaded records is compared with the source file.
The entities are also cached as parquet in temp folder in order to improve performance.

## Transformation
- Tables containing structure fields, are ingested with flattened schema
- Checkin table is ingested in a verticallized way
- All other tables are ingested without without changes.

## Error reporting

- The severe errors are saved to errors table in Cassandra. This table contains name of task, that has produced an error and error message.
- During ingest, child entities are checked against parent ones, if all foreign key constrainst are satisfied. All rows without parents, are not ingested.
Instead, IDs of such rows are inserted in key_errors table
- After ingest, the count of rows in ingested table is compared with the count of rows in source json file. If it differs, error is logged to DB and log file.