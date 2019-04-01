import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ValueZone {
  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    // check arguments
    if (args.length != 2)
      throw new IllegalArgumentException(
        "Parameters : "+
          "<rawRidesSource> <valueZonesTarget>"
      )

    logger.setLevel(Level.INFO)
    val session =
      SparkSession.builder
        .appName("value-zones")
        .getOrCreate()

    try {
      runJob(sparkSession = session,
        rawRides = args(0),
        valueZonesTarget = args(1)
      )
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.getStackTrace.toString)
    }
  }

  def runJob(sparkSession :SparkSession,rawRides :String, valueZonesTarget :String) = {

    logger.info("Execution started")

    import sparkSession.implicits._


    val allEventsWithZone = sparkSession.read
      .parquet(rawRides)
      .select("pickup_datetime","minute_rate","taxiColor","LocationID","Borough", "Zone")
      .cache

    val zoneAttractiveness = allEventsWithZone
      .groupBy($"LocationID", date_trunc("hour",$"pickup_datetime") as "pickup_hour")
      .pivot("taxiColor",Seq("yellow", "green"))
      .agg("minute_rate" -> "avg", "minute_rate" -> "count")
      .withColumnRenamed("yellow_avg(minute_rate)","yellow_avg_minute_rate")
      .withColumnRenamed("yellow_count(minute_rate)","yellow_count")
      .withColumnRenamed("green_avg(minute_rate)","green_avg_minute_rate")
      .withColumnRenamed("green_count(minute_rate)","green_count")

    val aggregateQuery = zoneAttractiveness
      .repartition(1)
      .sortWithinPartitions($"pickup_hour")
      .write.parquet(valueZonesTarget)

    sparkSession.stop()
  }
}