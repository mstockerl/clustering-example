import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

name := "clustering-example"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.7" % "provided",
  "org.elasticsearch" % "elasticsearch" % "1.4.4",
  "org.json4s" %% "json4s-core" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

assemblySettings

// disable testing
test in assembly := {}

jarName in assembly := "clustering-example.jar"

mainClass := Some("net.gutefrage.clustering.Main.class")
