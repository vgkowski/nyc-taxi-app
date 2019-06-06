import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object RawRide {

    private val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {

      // check arguments
      if (args.length != 3)
        throw new IllegalArgumentException(
          "Parameters : "+
          "<source> <color> <rawRidesTarget> "+
          "(multiple source paths can be provided in the same string, separated by a coma"
        )

      logger.setLevel(Level.INFO)
      val session =
          SparkSession.builder
            .appName("raw-rides")
            .getOrCreate()

      try {
        runJob(sparkSession = session,
              source = args(0).split(",").toList,
              color = args(1),
              target = args(2)
              )
        session.stop()
        } catch {
            case ex: Exception =>
              logger.error(ex.getMessage)
              logger.error(ex.getStackTrace.toString)
        }
    }

    def yellowEvents(spark: SparkSession)(df :DataFrame): DataFrame ={
      import spark.implicits._

      df.filter(col("tpep_pickup_datetime").gt("2017"))
        .filter(col("tpep_pickup_datetime").lt("2019"))
        .withColumn("duration", unix_timestamp($"tpep_dropoff_datetime").minus(unix_timestamp($"tpep_pickup_datetime")))
        .withColumn("minute_rate",$"total_amount".divide($"duration") * 60)
        .withColumnRenamed("tpep_pickup_datetime","pickup_datetime")
    }

    def greenEvents(spark : SparkSession)(df: DataFrame): DataFrame ={
      import spark.implicits._

      df.filter(col("lpep_pickup_datetime").gt("2017"))
        .filter(col("lpep_pickup_datetime").lt("2019"))
        .withColumn("duration", unix_timestamp($"lpep_dropoff_datetime").minus(unix_timestamp($"lpep_pickup_datetime")))
        .withColumn("minute_rate",$"total_amount".divide($"duration") * 60)
        .withColumnRenamed("lpep_pickup_datetime","pickup_datetime")
    }

    def processEvents(spark: SparkSession,color :String)(df :DataFrame): DataFrame ={
      color match{
        case "yellow" => yellowEvents(spark)(df)
        case "green" => greenEvents(spark)(df)
      }
    }

  def runJob(sparkSession :SparkSession,source :List[String], color :String, target :String) = {

      logger.info("Execution started")

      import sparkSession.implicits._

      val events = sparkSession.read
        .option("header","true")
        .option("inferSchema", "true")
        .option("enforceSchema", "false")
        .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("columnNameOfCorruptRecord", "error")
        .csv(source: _*)
        .transform(processEvents(sparkSession,color))

      val rawQuery = events
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