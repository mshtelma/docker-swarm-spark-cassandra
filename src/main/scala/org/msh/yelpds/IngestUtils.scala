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

  val errorDfStruct = StructType(
    Seq(
      StructField("table", StringType),
      StructField("field", StringType),
      StructField("value", StringType)
    )
  )

  def createEmptyDataFrame(spark: SparkSession, structType: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), structType)
  }

  def time[R](name: String)(block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("TASK:" + name + " Elapsed time: " + (t1 - t0) + "ms")
    result
  }

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


  def flattenSchema(df: DataFrame): DataFrame = {
    val columns = df.schema.fields.flatMap(p => {
      p.dataType match {
        case s: StructType => s.fields.map(x => df.col(p.name + "." + x.name))
        case _ => Seq(df.col(p.name))
      }
    })

    df.select(columns: _*)
  }


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

  def readJson(spark: SparkSession, path: String, fileName: String): DataFrame = {
    val df = spark.read.json(path + fileName)
    val flatDf = flattenSchema(df)
    flatDf
  }

  def cache(spark: SparkSession, df: DataFrame, name: String, path: String): DataFrame = {
    df.write.mode("overwrite").option("compression", "none").parquet(path + name + ".parquet")
    spark.read.parquet(path + name + ".parquet")
  }

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

  def addIndentityColumn(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    df.withColumn("id", monotonically_increasing_id)
  }

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

  def runCQLCommand(spark: SparkSession, cql: String): Unit = {
    CassandraConnector(spark.sparkContext).withSessionDo(s => s.execute(cql))
  }
}
