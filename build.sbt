name         := "forkulator"
version      := "1.0"
organization := "properbounds"
scalaVersion := "2.10.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "commons-cli" % "commons-cli" % "1.4-SNAPSHOT"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"
resolvers += Resolver.mavenLocal
