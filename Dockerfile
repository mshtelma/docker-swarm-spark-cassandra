FROM mshtelma/spark-base:latest

COPY target/yelpds-1.0-SNAPSHOT-jar-with-dependencies.jar $SPARK_HOME

WORKDIR $SPARK_HOME

CMD ./bin/spark-submit \
    --master spark://master:7077 \
    --driver-memory 3G  \
    --class org.msh.yelpds.IngestPipelineMain \
    yelpds-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --cassandra-host cassandra \
    --data-folder /data/ \
    --temp-folder /temp/
