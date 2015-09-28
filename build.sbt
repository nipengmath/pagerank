
name := "pagerank"

version := "0.0.1"

scalaVersion := "2.10.5"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
EclipseKeys.withSource := true

libraryDependencies ++= Seq( 
	"org.apache.spark" %% "spark-graphx" % "1.2.1" % "provided" 
)

