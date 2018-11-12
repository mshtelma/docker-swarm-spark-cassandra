package org.msh.yelpds

import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object IngestUtils {
  val logger = Logger(IngestUtils.getClass)

  import com.datastax.spark.connector._

  case class ParentFilterResult(filteredDf: DataFrame, errorDf: DataFrame)
  //Structure of error table
  val errorDfStruct = StructType(
    Seq(
      StructField("table", StringType),
      StructField("field", StringType),
      StructField("value", StringType)
    )
  )

  /**
    * Creates empty data frame with predefined schema
    * @param spark - spark session
    * @param structType - structure of the data frame
    * @return empty DF with given schema
    */
  def createEmptyDataFrame(spark: SparkSession, structType: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), structType)
  }

  /**
    * Logs duration of task
    * @param name - name of the task
    * @param block - lambda representing the task
    * @return any result returned by the task
    */
  def time[R](name: String)(block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("TASK:" + name + " Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  /**
    * runs lambda function in a try block and logs expeptions to DB
    * @param spark - spark session
    * @param keySpace - cassandra key space
    * @param name - task name
    * @param block - lambda function to run
    * @return VOID
    */
  def tryBlock[R](spark: SparkSession, keySpace: String, name: String)(block: => R): Any = {
    try {
      val result = block
      result
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        logExceptionToDB(spark, keySpace, name, e.getMessage)
    }
  }

  /**
    * Logs error message to DB
    * @param spark - spark session
    * @param keySpace - cassandra key space
    * @param title - title of task, which has produced exception
    * @param errorMessage - message to log
    */
  def logExceptionToDB(spark: SparkSession, keySpace: String, title: String, errorMessage: String): Unit = {
    try {
      runCQLCommand(spark, s"CREATE TABLE IF NOT EXISTS $keySpace.errors (" +
        "id UUID PRIMARY KEY, " +
        "process text, " +
        "error text " +
        ")")

      CassandraConnector(spark.sparkContext).withSessionDo(s => s.execute(
        s"insert into $keySpace.errors (id, process, error) values (now(), ?, ?)", title, errorMessage))
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  /**
    * Flattens structure fields  of DataFrame. Flattens only one level down.
    * @param df - dataframe
    * @return same dataframe but with flattened structure fields
    */
  def flattenSchema(df: DataFrame): DataFrame = {
    val columns = df.schema.fields.flatMap(p => {
      p.dataType match {
        case s: StructType => s.fields.map(x => df.col(p.name + "." + x.name))
        case _ => Seq(df.col(p.name))
      }
    })

    df.select(columns: _*)
  }

  /**
    * Enforces foreign key constraints for child entities. Rows, containing foreign key values, which do not exist
    * in parent entities will be filtered out and returned as errors
    * @param spark - Spark session
    * @param srcDf - child data frame
    * @param srcName - child entity name
    * @param parents - map of parent data frames
    * @return - filtered child data frame
    */
  def filterChildEntity(spark: SparkSession, srcDf: DataFrame, srcName: String, parents: Map[String, DataFrame]): ParentFilterResult = {
    val fkFields = srcDf.columns.filter(f => f.endsWith("_id"))
    val emptyDf = createEmptyDataFrame(spark, errorDfStruct)
    val res: ParentFilterResult = fkFields.foldLeft(ParentFilterResult(srcDf, emptyDf))((curr, name) => {
      val field = name.replace("_id", "")
      parents.get(field) match {
        case Some(parentDf) =>
          val currDf = curr.filteredDf
          val df = currDf.join(parentDf.select(name), Seq(name), "inner").drop(parentDf(name))
          val errorDf = currDf.join(parentDf.select(name), Seq(name), "leftanti")
            .select(lit(srcName) as "table", lit(name) as "field", currDf(name) as "value")
            .union(curr.errorDf)
          ParentFilterResult(df, errorDf)
        case None => curr
      }
    })
    ParentFilterResult(res.filteredDf, res.errorDf)
  }

  /**
    * Reads json file as Spark DF
    * @param spark - Spark session
    * @param path - folder with json file
    * @param fileName - json filename
    * @return - json data frame
    */
  def readJson(spark: SparkSession, path: String, fileName: String): DataFrame = {
    val df = spark.read.json(path + fileName)
    val flatDf = flattenSchema(df)
    flatDf
  }

  /**
    * Cache Data Frame as parquet on disk in temp folder
    * This makes spark break lineage and calculate the results and save them to disk
    * In a case of server deployment can be changed to caching in memory using spark capabilities of save parquet somewhere in memory
    * (e.g. in Alluxio)
    * @param spark - Spark session
    * @param df - data frame to cache
    * @param name - name of the entity
    * @param path - temp folder where the data frame will be cached
    * @return - cached data frame
    */
  def cache(spark: SparkSession, df: DataFrame, name: String, path: String): DataFrame = {
    df.write.mode("overwrite").option("compression", "none").parquet(path + name + ".parquet")
    spark.read.parquet(path + name + ".parquet")
  }

  /**
    * Ingests transformed Data Frame to Cassandra
    * @param spark - spark session
    * @param df - data frame to ingest
    * @param tableName - table name to ingest
    * @param keySpaceName - Cassandra Key Space
    * @param partitionColumns - Cassandra partitioning keys
    * @param clusteringColumns - Cassandra clustering keys
    * @return - ingested data frame (points to cassandra table, which was ingested)
    */
  def ingest(spark: SparkSession, df: DataFrame, tableName: String, keySpaceName: String,
             partitionColumns: Seq[String],
             clusteringColumns: Option[Seq[String]] = None): DataFrame = {
    time("Ingesting " + tableName) {
      import com.datastax.spark.connector._
      import com.datastax.spark.connector.cql._

      runCQLCommand(spark, "DROP TABLE IF EXISTS " + keySpaceName + "." + tableName)
      df.createCassandraTable(keySpaceName, tableName, partitionKeyColumns = Some(partitionColumns), clusteringKeyColumns = clusteringColumns)

      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
        .save()

      val resDf = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keySpaceName)) //"spark.cassandra.input.split.size_in_mb" -> "24"
        //.schema(StructType(Seq(StructField(partitionColumns.head, StringType))))
        .load()

      val newCount = resDf.count()
      val oldCount = df.count()
      //val diff = df.select(partitionColumns.head).except(resDf.select(partitionColumns.head)).count()
      if (oldCount != newCount) {
        println("Error: Loaded wrong number of rows for table " + tableName + " Expected: " + oldCount + " Actual: " + newCount)
      }
      resDf
    }
  }

  /**
    * Transform really wide and sparse checkin table in vertical form
    * @param spark - spark session
    * @param srcDf - source chein json data frame
    * @return - verticalized congested data frame
    */
  def congestCheckIn(spark: SparkSession, srcDf: DataFrame): DataFrame = {
    import spark.implicits._

    val congestedDf: Option[DataFrame] = srcDf.columns.filter(!_.equals("business_id")).foldLeft(None: Option[DataFrame])((dataFrameOption, name) => {
      val columnDf = srcDf.select($"business_id", lit(name) as "column", srcDf.col(name) as "value").where($"value".isNotNull)
      dataFrameOption match {
        case Some(df) => Some(df.union(columnDf))
        case None => Some(columnDf)
      }
    })
    congestedDf.head
  }

  /**
    * Add ID column to DataFrame
    * @param df - source DataFrame
    * @return - DataFrame with ID column
    */
  def addIndentityColumn(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    df.withColumn("id", monotonically_increasing_id)
  }

  /**
    * Saves row keys, that were filtered out, because of the missing parent rows to error table
    * @param spark - spark session
    * @param keySpaceName  - key space name
    * @param dfs - DataFrames with error rows to save
    */
  def saveKeyErrors(spark: SparkSession, keySpaceName: String, dfs: DataFrame*): Unit = {
    if (dfs.nonEmpty) {
      val errorsTableName = "key_errors"
      runCQLCommand(spark, "DROP TABLE IF EXISTS " + keySpaceName + "." + errorsTableName)

      val emptyDf = createEmptyDataFrame(spark, errorDfStruct)

      val df: DataFrame = addIndentityColumn(dfs.foldLeft(emptyDf)((unionDf, df) => unionDf.union(df)))

      df.createCassandraTable(keySpaceName, errorsTableName, partitionKeyColumns = Some(Seq("id")))
      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> errorsTableName, "keyspace" -> keySpaceName))
        .save()
    }
  }

  /**
    * run CQL statement against Cassandra cluster
    * @param spark - spark session to obtain connection to Cassandra
    * @param cql - statement to run
    */
  def runCQLCommand(spark: SparkSession, cql: String): Unit = {
    CassandraConnector(spark.sparkContext).withSessionDo(s => s.execute(cql))
  }
}
