name := "natoma-akka-tools"

version := "0.4.0"

scalaVersion := "2.13.7"
resolvers += Resolver.jcenterRepo

autoScalaLibrary := true

val akkaVersion = "2.6.17"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
)

// Test dependencies
val scalaTestVersion = "3.2.9"
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
)

Test / logBuffered := false
scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-opt:l:inline",
  "-opt-inline-from:**",
)
