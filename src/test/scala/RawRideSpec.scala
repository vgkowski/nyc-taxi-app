import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext, SparkSessionProvider}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.Path
import scala.util.Try

class RawRideSpec extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  override def beforeAll(): Unit = {
    super[SharedSparkContext].beforeAll()
    SparkSessionProvider._sparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  }

  test("yellowRawRides results test") {
    RawRide.runJob(spark,
      List("src/test/resources/yellow_tripdata_sample.csv"),
      "yellow",
      "src/test/resources/results/yellow"
    )

    val results = spark.read.parquet("src/test/resources/results/yellow/")

    val referenceResults = spark.read.parquet("src/test/resources/yellowRawRides/")

    assertDataFrameEquals(results, referenceResults)
  }

  test("greenRawRides results test") {
    RawRide.runJob(spark,
      List("src/test/resources/green_tripdata_sample.csv"),
      "green",
      "src/test/resources/results/green/"
    )

    val results = spark.read.parquet("src/test/resources/results/green/")

    val referenceResults = spark.read.parquet("src/test/resources/greenRawRides/")

    assertDataFrameEquals(results, referenceResults)
  }

  override def afterAll(): Unit = {
    Try(Path("src/test/resources/results/").deleteRecursively)
  }
}
