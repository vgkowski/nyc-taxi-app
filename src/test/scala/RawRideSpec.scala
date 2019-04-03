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

  test("full test") {
    RawRide.runJob(spark,
      List("src/test/resources/yellow_tripdata_sample.csv"),
      List("src/test/resources/green_tripdata_sample.csv"),
      List("src/test/resources/taxi_zone_lookup.csv"),
      "src/test/resources/results"
    )

    val results = spark.read.parquet("src/test/resources/results/")

    val referenceResults = spark.read.parquet("src/test/resources/reference/")

    assertDataFrameEquals(results, referenceResults)
  }

  override def afterAll(): Unit = {
    Try(Path("src/test/resources/results/").deleteRecursively)
    spark.stop()
  }
}
