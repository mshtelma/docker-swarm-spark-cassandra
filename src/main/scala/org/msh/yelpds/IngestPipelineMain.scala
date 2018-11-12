package org.msh.yelpds


import java.io.File

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.cassandra._
import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success, Try}


/**
  * Created by smikesh on 2018-11-04.
  */
object IngestPipelineMain extends App {
  val logger = Logger(IngestPipelineMain.getClass)

  import IngestUtils._

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val cassandraHost = opt[String](name = "cassandra-host", required = true)
    val dataFolder = opt[String](name = "data-folder", required = true)
    val tempFolder = opt[String](name = "temp-folder", required = true)
    verify()
  }

  try {
    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("Simple Application")
      // .master("local[4]")
      .config("spark.cassandra.connection.host", conf.cassandraHost.getOrElse("cassandra"))
      .getOrCreate()

    val keySpaceName = "yelp"
    val path = new File(conf.dataFolder.getOrElse("/data/")).getAbsolutePath + "/"
    val tempPath = new File(conf.dataFolder.getOrElse("/temp_data/")).getAbsolutePath + "/"

    // create key space if it does not exist
    runCQLCommand(spark, "CREATE KEYSPACE  IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':1};")
    //
    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    time("Yelp dataset pipeline, FULL LOAD ") {
      // at first parent entities are loaded. If any of them cannot be loaded, the whole pipeline must be stopped
      val parentDataFramesResult = Try {

        val businessDf = cache(spark, ingest(spark, readJson(spark, path, "yelp_academic_dataset_business.json"),
          "business", keySpaceName, Seq("business_id")), "business", tempPath)

        val userDf = cache(spark, ingest(spark, readJson(spark, path, "yelp_academic_dataset_user.json"),
          "user", keySpaceName, Seq("user_id")), "user", tempPath)

        (businessDf, userDf)
      }

      //now we can load all other entities
      parentDataFramesResult match {
        case Success(parentDataFrames) =>
          //get loaded parent entities
          val businessDf = parentDataFrames._1
          val userDf = parentDataFrames._2

          // Entity REVIEW
          tryBlock(spark, keySpaceName, "Loading REVIEW") {
            val reviewFilteredResult = filterChildEntity(spark, readJson(spark, path, "yelp_academic_dataset_review.json"),
              "review", Map("user" -> userDf, "business" -> businessDf))
            val reviewFilteredDf = cache(spark, reviewFilteredResult.filteredDf, "review", tempPath)
            val reviewDf = ingest(spark, reviewFilteredDf, "review", keySpaceName, Seq("review_id"),
              Some(Seq("user_id", "business_id")))
            saveKeyErrors(spark, keySpaceName, reviewFilteredResult.errorDf)
          }
          // Entity CHECKIN
          tryBlock(spark, keySpaceName, "Loading CHECKIN") {
            val checkinFilterResult = filterChildEntity(spark, readJson(spark, path, "yelp_academic_dataset_checkin.json"),
              "checkin", Map("business" -> businessDf))
            val congestedCheckinDf = cache(spark, congestCheckIn(spark, checkinFilterResult.filteredDf), "checkin", tempPath)
            val checkinDf = ingest(spark, congestedCheckinDf, "checkin",
              keySpaceName, Seq("business_id", "column"))
            saveKeyErrors(spark, keySpaceName, checkinFilterResult.errorDf)
          }
          // Entity PHOTO
          tryBlock(spark, keySpaceName, "Loading PHOTO") {
            val photoFilteredResult = filterChildEntity(spark, readJson(spark, path, "yelp_academic_dataset_photo.json"),
              "photo", Map("business" -> businessDf))
            val photoFilteredDf = cache(spark, photoFilteredResult.filteredDf, "photo", tempPath)
            val photoDf = ingest(spark, photoFilteredDf, "photo", keySpaceName, Seq("photo_id"), Some(Seq("business_id")))
            saveKeyErrors(spark, keySpaceName, photoFilteredResult.errorDf)
          }
          // Entity TIP
          tryBlock(spark, keySpaceName, "Loading TIP") {
            val tipFilteredResult = filterChildEntity(spark, readJson(spark, path, "yelp_academic_dataset_tip.json"), "tip",
              Map("user" -> userDf, "business" -> businessDf))
            val tipFilteredDf = cache(spark, addIndentityColumn(tipFilteredResult.filteredDf), "tip", tempPath)
            val tipDf = ingest(spark, tipFilteredDf, "tip", keySpaceName, Seq("user_id", "business_id", "id"))
            saveKeyErrors(spark, keySpaceName, tipFilteredResult.errorDf)
          }
        //log errors to DB occurred during loading of user and business entities
        case Failure(e) => logExceptionToDB(spark, keySpaceName, "Cannot load primary files (users, business)", e.getMessage)
      }


    }
  } catch {
    //log any other severe errors occurred during creation of spark session
    case e: Exception => logger.error(e.getMessage, e)
  }

}
