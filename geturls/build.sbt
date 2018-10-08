organization := "org.example"
name := "GetURLs"
version := "1.0"
scalaVersion := "2.11.0"
scalacOptions += "-target:jvm-1.7"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" 
libraryDependencies += "org.gdal" % "gdal" % "2.3.2"
         
