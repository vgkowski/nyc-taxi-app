import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MergeZoneRide {
    private val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {

      // check arguments
      if (args.length != 4)
        throw new IllegalArgumentException(
          "Parameters : "+
          "<yellowSource> <greenSource> <zonesSource> <mergedZonedRidesTarget> "
        )

      logger.setLevel(Level.INFO)
      val session =
          SparkSession.builder
            .appName("merge-zone-rides")
            .getOrCreate()

      try {
        runJob(sparkSession = session,
              yellow = args(0).split(",").toList,
              green = args(1).split(",").toList,
              zones = args(2).split(",").toList,
              target = args(3)
              )
        session.stop()
        } catch {
            case ex: Exception =>
              logger.error(ex.getMessage)
              logger.error(ex.getStackTrace.toString)
        }
    }

    def runJob(sparkSession :SparkSession,yellow :List[String], green :List[String], zones :List[String], target :String) = {

        logger.info("Execution started")

        import sparkSession.implicits._

        val yellowEvents = sparkSession.read
          .parquet(yellow: _*)
          .withColumn("taxiColor",lit("yellow"))

        val greenEvents = sparkSession.read
          .parquet(green: _*)
          .withColumn("taxiColor",lit("green"))

        val zonesInfo = sparkSession.read
            .option("header","true")
            .option("inferSchema", "true")
            .option("enforceSchema", "false")
            .option("columnNameOfCorruptRecord", "error")
            .csv(zones: _*)

        val allEventsWithZone = yellowEvents
            .withColumn("trip_type", lit(""))
            .withColumn("ehail_fee", lit(""))
            .union(greenEvents)
            .join(zonesInfo,$"PULocationID" === $"LocationID")
            .select("pickup_datetime","minute_rate","taxiColor","LocationID","Borough", "Zone")

        val mergedZonedQuery = allEventsWithZone
            .withColumn("year", year($"pickup_datetime"))
            .withColumn("month", month($"pickup_datetime"))
            .withColumn("day", dayofmonth($"pickup_datetime"))
            .repartition($"year",$"month")
            .sortWithinPartitions("day")
            .write
            .partitionBy("year","month")
            .parquet(target)
    }
}