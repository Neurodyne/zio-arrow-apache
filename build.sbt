val ZioVersion    = "1.0.0-RC11-1"
val Specs2Version = "4.7.0"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("snapshots")

organization := "Neurodyne"
name := "zio-serdes"
version := "0.0.1"
scalaVersion := "2.12.9"
maxErrors := 3
libraryDependencies ++= Seq(
  "dev.zio"    %% "zio"         % ZioVersion,
  "org.specs2" %% "specs2-core" % Specs2Version % "test"
)

scalacOptions --= Seq(
  "-Xfatal-warnings"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("chk", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
