/*
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.s3.model.DeleteObjectRequest

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
object RawRideSpecLive extends org.specs2.mutable.Specification {

  def getClusterToTestId(count: Int = 0) : Option[(String)] = {
    val clustersReady = emrClient
      .listClusters(new ListClustersRequest()
        .withClusterStates("WAITING"))
       .getClusters()
      .filter(_.getName.equals("demoClustertest"))
    if ((clustersReady.isEmpty) && (count < 5)) {
      Thread.sleep(300000)
      getClusterToTestId(count +1)
    } else if (clustersReady.isEmpty) {
      None
    } else {
      Some(clustersReady.head.getId)
    }
  }

  def getStepStatus(stepId: String, clusterId: String, count: Int = 0) : String = {
    val status = emrClient.describeStep(new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId)).getStep.getStatus.getState
    if (List("PENDING","RUNNING").contains(status) && (count < 60)){
      Thread.sleep(60000)
      getStepStatus(stepId, clusterId, count +1)
    } else {
      status
    }
  }

  val emrClient = AmazonElasticMapReduceClientBuilder.defaultClient()

  val s3client = AmazonS3ClientBuilder.defaultClient()

  "this is a simple specification" >> {
    "where a step of emr must execute successfully" >> {
      val clusterId = getClusterToTestId()
      clusterId.isDefined must_==  true
      val testJar = emrClient
        .describeCluster(new DescribeClusterRequest()
          .withClusterId(clusterId.get))
        .getCluster.getTags
        .filter(_.getKey.equals("DefaultJob"))
        .head.getValue
      val bucketName = testJar.split("/")(2)
      List("liveresults/part-00000","liveresults/part-00001","liveresults/_SUCCESS")
        .foreach(key => s3client.deleteObject(new DeleteObjectRequest(bucketName, key)))
      val args = List("/usr/bin/spark-submit",
        "--master",
        "yarn-cluster",
        "--class",
        "DemoClass",
        testJar,
        "s3://lfcarocomdemo/index.html",
        s"s3://$bucketName/liveresults")
      val stepConfig = new HadoopJarStepConfig()
        .withJar("s3n://elasticmapreduce/libs/script-runner/script-runner.jar")
        .withArgs(args)
      val step = new StepConfig().withName("SparkLiveTest1").withHadoopJarStep(stepConfig).withActionOnFailure("CONTINUE")
      val result = emrClient.addJobFlowSteps(new AddJobFlowStepsRequest()
        .withJobFlowId(clusterId.get)
        .withSteps(step))
      val stepId = result.getStepIds.map(_.toString).head
      getStepStatus(stepId, clusterId.get) must_==  "COMPLETED"
      List("liveresults/part-00000","liveresults/part-00001","liveresults/_SUCCESS")
        .map(key => s3client.doesObjectExist(bucketName, key)) must_== List(true,true,true)
    }
  }

}*/