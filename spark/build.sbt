name         := "forkulator"
version      := "2.0"
organization := "ikt"
scalaVersion := "2.12.10"

/**
  * Makes it possible to run the application from sbt-shell.
  * Otherwise java.lang.InterruptedException gets called and
  * the context isn't closed correctly
  */
fork         := true

libraryDependencies += "org.apache.spark" %% "spark-core"  % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "3.0.1"
libraryDependencies += "commons-cli" % "commons-cli" % "1.4"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

resolvers += Resolver.mavenLocal
//mainClass in assembly := Some("forkulator.FJSimulator")

// because this is a java project
autoScalaLibrary := false
crossPaths := false

// if you are using sbt-eclipse, tell it that this is a java project
//EclipseKeys.projectFlavor := EclipseProjectFlavor.Java

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
