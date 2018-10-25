organization := "org.example"
name := "BandCombination"
version := "1.0"
scalaVersion := "2.11.0"
scalacOptions += "-target:jvm-1.7"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.1.1",
	"com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.2",
	"com.azavea.geotrellis" %% "geotrellis-util" % "0.10.2",
	"com.azavea.geotrellis" %% "geotrellis-raster" % "0.10.2",
	"com.esotericsoftware.kryo" % "kryo" % "2.21",
	 "com.azavea.geotrellis" %% "geotrellis-proj4" % "0.10.2"
)



