addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

// no sbt 1.1 version yet
resolvers ++= Seq( Resolver.bintrayIvyRepo("kamon-io", "sbt-plugins"))
addSbtPlugin("io.kamon" % "sbt-aspectj-runner" % "1.1.1")

// get aspectj weaver into assembly fat JARs
addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.4")
