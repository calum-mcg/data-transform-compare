ThisBuild / scalaVersion := "2.12.14" // to match Dataproc latest version
javacOptions ++= Seq("-source", "11", "-target", "11") // Ensure this aligns with version set with SDKMAN
val sparkVersion = "3.1.3" // to match Dataproc latest version
ThisBuild / organization := "com.stackoverflow"

logLevel := Level.Error

lazy val aggregate_users = (project in file("."))
  .settings(
    name := "AggregateUsers",
    libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
  )