scalaVersion := "2.11.12"

val sparkVersion = "2.1.2"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
  "org.typelevel" %% "cats-core" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
