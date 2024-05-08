
scalaVersion := "2.12.16"

val sparkVersion = "3.3.0"


lazy val root = (project in file("."))
  .configs(Test)
  .settings(
    inThisBuild(List(
      organization := "com.thoughtworks.cd.de",
      scalaVersion := "2.12.16",
      version := "0.1.0-SNAPSHOT"
    )),
    Test / scalaSource := baseDirectory.value / "src/test/it",
    name := "tw-pipeline",
    ThisBuild / libraryDependencySchemes ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.typesafe" % "config" % "1.4.2",
      "au.com.bytecode" % "opencsv" % "2.4",
      "org.scalatest" %% "scalatest" % "3.2.12",
      "junit" % "junit" % "4.13.2",
    )
  )

