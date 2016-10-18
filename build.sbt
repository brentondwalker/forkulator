name         := "forkulator"
version      := "1.0"
organization := "properbounds"
scalaVersion := "2.10.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2" % "provided"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
resolvers += Resolver.mavenLocal
//mainClass in assembly := Some("forlulator.FJSimulator")
<<<<<<< HEAD

// because this is a java project
autoScalaLibrary := false
crossPaths := false

// if you are using sbt-eclipse, tell it that this is a java project
EclipseKeys.projectFlavor := EclipseProjectFlavor.Java
=======
>>>>>>> 07d60c134b005d092d970e45f8b2d364035fd7c5
