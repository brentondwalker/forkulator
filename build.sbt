name         := "forkulator"
version      := "1.0"
organization := "properbounds"
scalaVersion := "2.10.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2" % "provided"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
resolvers += Resolver.mavenLocal
//mainClass in assembly := Some("forlulator.FJSimulator")

// because this is a java project
autoScalaLibrary := false
crossPaths := false

// if you are using sbt-eclipse, tell it that this is a java project
EclipseKeys.projectFlavor := EclipseProjectFlavor.Java
