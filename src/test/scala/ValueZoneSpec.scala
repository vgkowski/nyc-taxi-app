import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext, SparkSessionProvider}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.Path
import scala.util.Try

class ValueZoneSpec extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  override def beforeAll(): Unit = {
    super[SharedSparkContext].beforeAll()
    SparkSessionProvider._sparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  }

  test("valueZones results test") {
    ValueZone.runJob(spark,
      "src/test/resources/mergeZoneRides",
      "src/test/resources/results/valueZones"
    )

    val results = spark.read.parquet("src/test/resources/results/valueZones")

    val referenceResults = spark.read.parquet("src/test/resources/valueZones/")

    assertDataFrameEquals(results, referenceResults)
  }

  override def afterAll(): Unit = {
    //Try(Path("src/test/resources/results/").deleteRecursively)
  }
}
