import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext, SparkSessionProvider}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.Path
import scala.util.Try

class MergeZoneRideSpec extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  override def beforeAll(): Unit = {
    super[SharedSparkContext].beforeAll()
    SparkSessionProvider._sparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  }

  test("mergeZoneRides results test") {
    MergeZoneRide.runJob(spark,
      List("src/test/resources/yellowRawRides/"),
      List("src/test/resources/greenRawRides/"),
      List("src/test/resources/taxi_zone_lookup.csv"),
      "src/test/resources/results/mergeZone"
    )

    val results = spark.read.parquet("src/test/resources/results/mergeZone")

    val referenceResults = spark.read.parquet("src/test/resources/mergeZoneRides/")

    assertDataFrameEquals(results, referenceResults)
  }

  override def afterAll(): Unit = {
    Try(Path("src/test/resources/results/").deleteRecursively)
  }
}
