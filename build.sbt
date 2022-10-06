ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
        .settings(
            name := "ecs_s3_trigger_lambda"
        )

assemblyJarName in assembly := "gex-ecs-s3-trigger-lambda.jar"

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-lambda-java-core" % "1.2.1",
    "com.amazonaws" % "aws-lambda-java-events" % "3.11.0",
    "com.amazonaws" % "aws-java-sdk-ecs" % "1.12.277",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.12.293",
    "commons-logging" % "commons-logging" % "1.2",
    "com.google.code.gson" % "gson" % "2.9.1"
)

Compile / mainClass := Some("com.gex.Entry::handler")

