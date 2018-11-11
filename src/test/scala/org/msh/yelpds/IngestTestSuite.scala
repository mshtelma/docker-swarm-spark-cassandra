package org.msh.yelpds

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._
import org.apache.spark.sql.types._

class IngestTestSuite extends SparkFunSuite with SparkTemplate with BeforeAndAfterAll with EmbeddedCassandra {

  import IngestUtils._

  override def clearCache(): Unit = CassandraConnector.evictCache()

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)


  override def beforeAll(): Unit = {
    super.beforeAll()
    connector.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    }
  }

  test("Test IngestUtils.logExceptionToDB ") {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql._

    connector.withSessionDo { session =>
      session.execute("DROP TABLE IF EXISTS test.errors;")
    }
    logExceptionToDB(spark, "test", "myprocess", "my exception")
    connector.withSessionDo { session =>
      val rs = session.execute("select process, error from test.errors;")
      val row = rs.one()

      assertResult("myprocess") {
        row.getString("process")
      }
      assertResult("my exception") {
        row.getString("error")
      }
    }

  }

  test("Test IngestUtils.filterChildEntity ") {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql._


    val parentDf = spark.range(1, 100).select('id as "parent_id", concat(lit("p"), 'id))
    val childDf = spark.range(50, 120).select('id as "child_id", 'id as "parent_id")

    val res = filterChildEntity(spark, childDf, "parent", Map("parent" -> parentDf))

    assertResult(20) {
      res.errorDf.count()
    }
    assertResult(50) {
      res.filteredDf.count()
    }
  }


  test("Test IngestUtils.congestCheckIn") {
    implicit val spark: SparkSession = sparkSession

    val lines = List(
      Row("id1", "1", null, null),
      Row("id2", null, "2", null),
      Row("id3", null, null, "3"),
      Row("id4", "11", null, "4")
    )

    val schema = StructType(
      Seq(
        StructField("business_id", StringType),
        StructField("f1", StringType),
        StructField("f2", StringType),
        StructField("f3", StringType)
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(lines), schema = schema)
    val congestedDF = congestCheckIn(spark, df)
    assertResult(6) {
      congestedDF.count()
    }
  }

  test("Test IngestUtils.ingest") {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val lines = List(
      Row("id1", "1", "ss", 22),
      Row("id2", "we", "ewq", 43),
      Row("id3", "dss", "wer", 45)
    )

    val schema = StructType(
      Seq(
        StructField("business_id", StringType),
        StructField("f1", StringType),
        StructField("f2", StringType),
        StructField("f3", IntegerType)
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(lines), schema = schema)

    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    }
    ingest(spark, df, "testtable", "test", partitionColumns = Seq("business_id"))

    val resDf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "testtable", "keyspace" -> "test"))
      .load()

    assertResult(3) {
      resDf.count()
    }
  }


}
