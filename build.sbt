import CommonDependency.dependencies

organization in ThisBuild := "io.github.junheng.akka"

scalaVersion in ThisBuild := "2.11.7"

lazy val protocol = project
  .settings(
    name := "akka-kafka-protocol",
    version := "0.1-SNAPSHOT"
  )


lazy val proxy = project
  .dependsOn(service)
  .settings(
    name := "akka-kafka-proxy",
    version := "0.1-SNAPSHOT"
  )

lazy val service = project
  .dependsOn(protocol)
  .settings(
    name := "akka-kafka-service",
    version := "0.1-SNAPSHOT",
    libraryDependencies ++= dependencies.common,
    libraryDependencies ++= dependencies.akka,
    libraryDependencies ++= dependencies.kafka
  )
