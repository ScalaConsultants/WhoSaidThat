name := "who-said-that"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)