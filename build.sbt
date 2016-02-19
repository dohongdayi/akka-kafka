import CommonDependency.dependencies

organization in ThisBuild := "io.github.junheng.akka"

scalaVersion in ThisBuild := "2.11.7"

lazy val protocol = project
  .settings(
    name := "akka-kafka-protocol",
    version := "0.11-SNAPSHOT",
    libraryDependencies ++= dependencies.common
  )


lazy val proxy = project
  .dependsOn(service, protocol)
  .enablePlugins(JavaServerAppPackaging)
  .settings(
    name := "akka-kafka-proxy",
    version := "0.12-SNAPSHOT",
    libraryDependencies ++= dependencies.logs,
    libraryDependencies ++= Seq(
      "io.github.junheng.akka" %% "akka-accessor" % "0.1-SNAPSHOT" withSources(),
      "io.github.junheng.akka" %% "akka-locator" % "0.1-SNAPSHOT" withSources(),
      "io.github.junheng.akka" %% "akka-monitor" % "0.1-SNAPSHOT" withSources(),
      "io.github.junheng.akka" %% "akka-utils" % "0.1-SNAPSHOT" withSources()
    )
  )


lazy val service = project
  .dependsOn(protocol)
  .settings(
    name := "akka-kafka-service",
    version := "0.12-SNAPSHOT",
    libraryDependencies ++= dependencies.akka,
    libraryDependencies ++= dependencies.kafka
  )
