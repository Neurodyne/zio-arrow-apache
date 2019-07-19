val ApacheSerdresVersion = "3.9"
val ZioVersion           = "1.0.0-RC10-1"
val Specs2Version        = "4.6.0"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("snapshots")

organization := "Neurodyne"
name := "zio-serdes"
version := "0.0.1"
scalaVersion := "2.12.8"
maxErrors := 3
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % ApacheSerdresVersion,
  "dev.zio"            %% "zio"          % ZioVersion,
  "org.specs2"         %% "specs2-core"  % Specs2Version % "test"
)

scalacOptions --= Seq(
  "-Xfatal-warnings"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("chk", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
