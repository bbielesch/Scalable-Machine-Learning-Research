name := "Machine Learning"

version := "1.0"

scalaVersion := "2.11.8"

//libraryDependencies += groupID % artifactID % revision

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"
libraryDependencies += "org.rogach" %% "scallop" % "2.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
//libraryDependencies += "ch.cern" % "sparkmeasure" % "0.1"
