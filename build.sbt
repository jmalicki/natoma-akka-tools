name := "natoma-akka-tools"

version := "0.1"

scalaVersion := "2.12.7"
resolvers += Resolver.jcenterRepo

autoScalaLibrary := true

val akkaVersion = "2.5.18"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.github.nscala-time" %% "nscala-time" % "2.20.0",
)

// Find errors in scalatest
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.7")

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
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
