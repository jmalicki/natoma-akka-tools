name := "natoma-akka-tools"

version := "0.3.1"

crossScalaVersions := Seq("2.12.9", "2.13.0")
resolvers += Resolver.jcenterRepo

autoScalaLibrary := true

val akkaVersion = "2.5.25"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
)

logBuffered in Test := false
scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-opt:l:inline",
  "-opt-inline-from:**",
)
