package com.gex

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.ecs.model.{AssignPublicIp, AwsVpcConfiguration, NetworkConfiguration, RunTaskRequest}
import com.amazonaws.services.ecs.{AmazonECS, AmazonECSClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.gex.HttpHandler.Notification

object EcsTaskHandler {
    val client: AmazonECS = AmazonECSClientBuilder.standard.withClientConfiguration(new ClientConfiguration()).build()
    val clusterName: String = Config.clusterName
    val subnet: String = Config.subnet
    val securityGroup: String = Config.securityGroup

    def triggerTask(taskName: String): Response = {

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
                runTaskRequest.setTaskDefinition(Config.umitoolsExtractTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "umitools_dedup" =>
                runTaskRequest.setTaskDefinition(Config.umitoolsDedupTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "htseq_count" =>
                runTaskRequest.setTaskDefinition(Config.htseqCountTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "samtools_index" =>
                runTaskRequest.setTaskDefinition(Config.samtoolsIndexTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "bbduk" =>
                runTaskRequest.setTaskDefinition(Config.bbdukTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "star" =>
                runTaskRequest.setTaskDefinition(Config.starTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case "fastqc" =>
                runTaskRequest.setTaskDefinition(Config.fastqcTaskDefFamily)
                runTask(runTaskRequest)
                Response()
            case _ => Response(s"unable to find task $taskName. Valid tasks are [umitools_extract, umitools_dedup, htseq_count, samtools_index, bbduk, star, fastqc]", 404)
        }
    }

    def runTask(request: RunTaskRequest): Unit = {
        client.runTask(request)
    }

    def createEnvFileInS3(envFileName: String = "env_file.env", inputFileName: String): Unit = {
        println(s"writing $envFileName with FILENAME $inputFileName")
        val s3Client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()
        s3Client.putObject("gex-fargate-bucket", s"fargate-inputs/$envFileName", s"FILENAME=$inputFileName")
    }

    def handleS3PutEvent(key: String): Unit = {
        val fileName = key.substring(key.indexOf("/") + 1, key.length)
        val prefix = fileName.substring(0, fileName.indexOf("."))
        val suffix = fileName.substring(fileName.indexOf(".") + 1)
        val (previousTaskName, taskName, inputFileName) = getNextJobName(prefix, suffix)
        taskName match {
            case "invalid" => println(s"Nothing to do for $fileName")
            case _ => println(s"triggering $taskName from s3 input $fileName")
                createEnvFileInS3(inputFileName = inputFileName)
                triggerTask(taskName)
                logNotificationResponse(previousTaskName, "COMPLETED", HttpHandler.sendTaskNotification(Notification(previousTaskName, "COMPLETED")))
                logNotificationResponse(taskName, "STARTING", HttpHandler.sendTaskNotification(Notification(taskName, "STARTING")))

        }
    }

    private def logNotificationResponse(taskName: String, status: String, response: Response): Unit = {
        response match {
            case Response(_, 200) => println(s"successfully sent notification for task name $taskName and status $status")
            case Response(message, code) if code != 200 => println(s"failed to send notification for task name $taskName ans status $status. Code $code message $message")
            case _ => println("unknown error")
        }
    }

    def getNextJobName(prefix: String, suffix: String): (String, String, String) = {
        suffix match {
            case "extracted.fastq.gz" => ("umitools_extract", "bbduk", prefix + ".extracted.fastq.gz")
            case "bbduk.fastq.gz" => ("bbduk", "star", prefix + ".bbduk.fastq.gz")
            case "Aligned.sortedByCoord.out.bam" => ("star", "htseq_count", prefix + ".Aligned.sortedByCoord.out.bam")
            case "Aligned.sortedByCoord.out.table.txt" => ("htseq_count", "umitools_dedup", prefix + ".Aligned.sortedByCoord.out.bam")
            case "Aligned.sortedByCoord.out.deduplicated.bam" => ("umitools_dedup", "htseq_count", prefix + ".Aligned.sortedByCoord.out.deduplicated.bam")
            case "Aligned.sortedByCoord.out.deduplicated.table.txt" => ("htseq_count", "fastqc", prefix + ".Aligned.sortedByCoord.out.bam")
            case "Aligned.sortedByCoord.out_fastqc.zip" => ("fastqc", "fastqc", prefix + ".Aligned.sortedByCoord.out.deduplicated.bam")
            case _ => ("invalid", "invalid", "invalid")
        }
    }

    case class Response(text: String = "okay", code: Int = 200)
}
