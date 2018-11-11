# Ingesting json data to Cassandra using Spark SQL

This project illustrates how simple json dataset (using Yelp dataset as an example) can be ingested in Cassandra using Spark SQL. 
The whole environment can be deployed using Docker Swarm. 

The project can be run using the following steps:

- Initialize Docker Swarm using 
`docker swarm init`
- Start stack including Cassandra node, Spark SQL cluster with three workers
`sh start-stack.sh `
- Create a service that will run the actual pipeline (the script accepts path to tar.gz archive with the Yelp dataset)
`sh start-job.sh ~/Downloads/yelp_dataset.tar.gz `

The scripts are going to save temporary files in /tmp filesystem, so this means, that the read/write permissions for the tmp filesyste are required. 


