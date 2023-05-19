package com.gex

import com.amazonaws.services.ecs.model._

import scala.jdk.CollectionConverters.{ MapHasAsJava, SeqHasAsJava }

object ContainerDef {

  def create(
      name: String,
      accountId: String,
      envFileName: String,
      workspace: String = "dev",
      cpu: String = "1024",
      mem: String = "2048",
      ephemMem: Int = 22
  ): RegisterTaskDefinitionRequest = {

    val pm = new PortMapping()
    pm.setContainerPort(8080)
    pm.setHostPort(8080)
    pm.setProtocol(TransportProtocol.Tcp)

    val envFile = new EnvironmentFile()
    envFile.setType("s3")
    envFile.setValue(
      s"arn:aws:s3:::gex-fargate-bucket/fargate-inputs/$envFileName"
    )

    val logConf = new LogConfiguration()
    logConf.setLogDriver(LogDriver.Awslogs)
    logConf.setOptions(
      Map(
        "awslogs-group"         -> "ecs",
        "awslogs-region"        -> "us-east-1",
        "awslogs-stream-prefix" -> name
      ).asJava
    )

    val nameTag = new Tag()
    nameTag.setKey("name")
    nameTag.setValue(name)

    val workspaceTag = new Tag()
    workspaceTag.setKey("workspace")
    workspaceTag.setValue(workspace)

    val containerDefinition: ContainerDefinition = new ContainerDefinition()
    containerDefinition.setName(name)
    containerDefinition.setImage(
      s"$accountId.dkr.ecr.us-east-1.amazonaws.com/$name-repo-$workspace:latest"
    )
    containerDefinition.setEssential(true)
    containerDefinition.setPortMappings(List(pm).asJava)
    containerDefinition.setEnvironmentFiles(List(envFile).asJava)
    containerDefinition.setLogConfiguration(logConf)

    val taskDefinition: RegisterTaskDefinitionRequest =
      new RegisterTaskDefinitionRequest()

    taskDefinition.setFamily(s"$name-$workspace")
    taskDefinition.setRequiresCompatibilities(
      List(Compatibility.FARGATE.toString).asJava
    )
    taskDefinition.setCpu(cpu)
    taskDefinition.setMemory(mem)
    taskDefinition.setNetworkMode(NetworkMode.Awsvpc)
    taskDefinition.setExecutionRoleArn(
      "arn:aws:iam::287730706223:role/ecsTaskExecutionRole"
    )
    val storage = new EphemeralStorage()
    taskDefinition.setEphemeralStorage(storage.withSizeInGiB(ephemMem))
    taskDefinition.setContainerDefinitions(List(containerDefinition).asJava)
    taskDefinition.setTags(List(nameTag, workspaceTag).asJava)

    taskDefinition
  }
}
