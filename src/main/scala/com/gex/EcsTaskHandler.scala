package com.gex

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.ecs.model.{ AssignPublicIp, AwsVpcConfiguration, NetworkConfiguration, RunTaskRequest }
import com.amazonaws.services.ecs.{ AmazonECS, AmazonECSClientBuilder }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import com.gex.HttpHandler.Notification

object EcsTaskHandler {
  val client: AmazonECS     = AmazonECSClientBuilder.standard.withClientConfiguration(new ClientConfiguration()).build()
  val clusterName: String   = Config.clusterName
  val subnet: String        = Config.subnet
  val securityGroup: String = Config.securityGroup

  def triggerTask(
      taskName: String,
      envFileName: String = "env_file.env",
      accountId: String = "287730706223"
  ): Response = {

    println(s"running task - $taskName")

    val vpcConfiguration: AwsVpcConfiguration = new AwsVpcConfiguration()
      .withSubnets(subnet)
      .withAssignPublicIp(AssignPublicIp.ENABLED)
      .withSecurityGroups(securityGroup)
      .withAssignPublicIp(AssignPublicIp.ENABLED)
    val networkConfiguration: NetworkConfiguration = new NetworkConfiguration()
    networkConfiguration.setAwsvpcConfiguration(vpcConfiguration)

    val runTaskRequest = new RunTaskRequest()
    runTaskRequest.setCluster(clusterName)
    runTaskRequest.setLaunchType("FARGATE")
    runTaskRequest.setNetworkConfiguration(networkConfiguration)

    taskName.toLowerCase() match {
      case "umitools_extract" =>
        client.registerTaskDefinition(ContainerDef.create("gex-umitools-extract", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.umitoolsExtractTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "umitools_dedup" => // empty
        client.registerTaskDefinition(ContainerDef.create("gex-umitools-dedup", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.umitoolsDedupTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "htseq_count" =>
        client.registerTaskDefinition(ContainerDef.create("gex-htseq-count", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.htseqCountTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "samtools_index" => // empty
        client.registerTaskDefinition(ContainerDef.create("gex-samtools-index", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.samtoolsIndexTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "bbduk" =>
        client.registerTaskDefinition(ContainerDef.create("gex-bbduk", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.bbdukTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "star" =>
        client.registerTaskDefinition(
          ContainerDef.create("gex-star", accountId, envFileName, cpu = "4096", mem = "30720", ephemMem = 100)
        )
        runTaskRequest.setTaskDefinition(Config.starTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "fastqc" =>
        client.registerTaskDefinition(ContainerDef.create("gex-fastqc", accountId, envFileName))
        runTaskRequest.setTaskDefinition(Config.fastqcTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case "total_counts" =>
        client.registerTaskDefinition(ContainerDef.create("gex-total-counts", accountId, envFileName, cpu = "4096", mem = "30720"))
        runTaskRequest.setTaskDefinition(Config.totalCountsTaskDefFamily)
        runTask(runTaskRequest)
        Response()
      case _ =>
        Response(
          s"unable to find task $taskName. Valid tasks are [umitools_extract, umitools_dedup, htseq_count, samtools_index, bbduk, star, fastqc, total_counts]",
          404
        )
    }
  }

  def runTask(request: RunTaskRequest): Unit = {
    client.runTask(request)
  }

  def createEnvFileInS3(subfolder: String, inputFileName: String, envFileName: String = "env_file.env"): Unit = {
    println(s"writing $envFileName with FILENAME $inputFileName and subfolder $subfolder")
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()
    s3Client.putObject(
      "gex-fargate-bucket",
      s"fargate-inputs/$envFileName",
      s"SUBFOLDER=$subfolder\nFILENAME=$inputFileName"
    )
  }

  def parseKey(fullKey: String): (String, String) = {
    val key       = fullKey.substring(fullKey.indexOf("/") + 1)
    def subfolder = key.substring(0, key.indexOf("/"))
    def fileName  = key.substring(key.indexOf("/") + 1)

    if (fullKey.count(_ == '/') == 2) (subfolder, fileName) else ("", key)

  }

  def handleS3PutEvent(key: String): Unit = {
    val (subfolder, fileName)                           = parseKey(key)
    val prefix                                          = fileName.substring(0, fileName.indexOf("."))
    val suffix                                          = fileName.substring(fileName.indexOf(".") + 1)
    val (previousTaskName, nextTaskName, inputFileName) = getNextJobName(prefix, suffix)
    nextTaskName match {
      case List("invalid") => println(s"Nothing to do for $fileName")
      case _ =>
        println(s"triggering $nextTaskName from s3 input $fileName")
        nextTaskName.foreach { taskName =>
          if (taskName == "total_counts") {
            val envFileNameTotalCounts = s"env_file_${subfolder}_total_counts.env"
            createEnvFileInS3(subfolder = subfolder, inputFileName = s"$prefix.Aligned.sortedByCoord.out.deduplicated.table.txt", envFileName = envFileNameTotalCounts)
            triggerTaskAndNotify(taskName, previousTaskName, envFileNameTotalCounts)
          } else {
            val envFileName = s"env_file_$subfolder.env"
            createEnvFileInS3(subfolder = subfolder, inputFileName = inputFileName, envFileName = envFileName)
            triggerTaskAndNotify(taskName, previousTaskName, envFileName)
          }
        }
    }
  }

  private def triggerTaskAndNotify(nextTaskName: String, previousTaskName: String, envFileName: String): Unit = {
    triggerTask(nextTaskName, envFileName)
    logNotificationResponse(
      previousTaskName,
      "COMPLETED",
      HttpHandler.sendTaskNotification(Notification(previousTaskName, "COMPLETED"))
    )
    logNotificationResponse(
      nextTaskName,
      "STARTING",
      HttpHandler.sendTaskNotification(Notification(nextTaskName, "STARTING"))
    )
  }

  private def logNotificationResponse(taskName: String, status: String, response: Response): Unit = {
    response match {
      case Response(_, 200) => println(s"successfully sent notification for task name $taskName and status $status")
      case Response(message, code) if code != 200 =>
        println(s"failed to send notification for task name $taskName ans status $status. Code $code message $message")
      case _ => println("unknown error")
    }
  }

  def getNextJobName(prefix: String, suffix: String): (String, List[String], String) = {
    suffix match {
      case "extracted.fastq.gz"            => ("umitools_extract", List("bbduk"), prefix + ".extracted.fastq.gz")
      case "bbduk.fastq.gz"                => ("bbduk", List("star"), prefix + ".bbduk.fastq.gz")
      case "Aligned.sortedByCoord.out.bam" => ("star", List("htseq_count"), prefix + ".Aligned.sortedByCoord.out.bam")
      case "Aligned.sortedByCoord.out.table.txt" =>
        ("htseq_count", List("umitools_dedup"), prefix + ".Aligned.sortedByCoord.out.bam")
      case "Aligned.sortedByCoord.out.deduplicated.bam" =>
        ("umitools_dedup", List("htseq_count"), prefix + ".Aligned.sortedByCoord.out.deduplicated.bam")
      case "Aligned.sortedByCoord.out.deduplicated.table.txt" =>
        ("htseq_count", List("fastqc", "total_counts"), prefix + ".Aligned.sortedByCoord.out.bam")
      case "Aligned.sortedByCoord.out_fastqc.zip" =>
        ("fastqc", List("fastqc"), prefix + ".Aligned.sortedByCoord.out.deduplicated.bam")
      case _ => ("invalid", List("invalid"), "invalid")
    }
  }

  case class Response(text: String = "okay", code: Int = 200)
}
