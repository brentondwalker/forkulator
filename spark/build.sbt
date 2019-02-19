name         := "forkulator"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.3.2"
//libraryDependencies += "org.apache.spark" % "spark-mllib-local_2.11" % "2.3.2"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

resolvers += Resolver.mavenLocal
//mainClass in assembly := Some("forlulator.FJSimulator")

// because this is a java project
autoScalaLibrary := false
crossPaths := false

// if you are using sbt-eclipse, tell it that this is a java project
EclipseKeys.projectFlavor := EclipseProjectFlavor.Java

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
