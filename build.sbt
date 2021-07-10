name := "spark-covid"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1" % "provided"
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "7.9.2"