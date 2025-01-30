name := "spark-stepik"

version := "0.1"

scalaVersion := "2.12.18"

lazy val sparkVersion = "3.5.4"
val circeVersion = "0.14.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "log4j" % "log4j" % "1.2.17",
  "io.circe" %% "circe-core"    % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser"  % circeVersion,
  "com.typesafe" % "config" % "1.4.3",
  "com.github.scopt" %% "scopt" % "4.0.1"
)

// Настройки для sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

assemblyJarName in assembly := "final.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard 
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "log4j.properties" => MergeStrategy.first
  case x if x.endsWith(".html") || x.endsWith(".xml") => MergeStrategy.first
  case _ => MergeStrategy.first
}
