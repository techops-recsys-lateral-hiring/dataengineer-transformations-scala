
scalaVersion := "2.13.17"

val sparkVersion = "4.0.1"


lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    inThisBuild(List(
      organization := "com.thoughtworks.cd.de",
      scalaVersion := "2.13.17",
      version := "0.1.0-SNAPSHOT"
    )),
    Defaults.itSettings,
    name := "tw-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2",
      "au.com.bytecode" % "opencsv" % "2.4",
      "org.scalatest" %% "scalatest" % "3.2.12" % "it,test",
      "junit" % "junit" % "4.13.2" % Test,

    )
  )

