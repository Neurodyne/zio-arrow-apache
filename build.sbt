val ZioVersion    = "1.0.0-RC12-1"
val Specs2Version = "4.7.0"
val ArrowVersion  = "0.14.1"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("snapshots")

organization := "Neurodyne"
name := "zio-serdes"
version := "0.0.1"
scalaVersion := "2.12.9"
maxErrors := 3
libraryDependencies ++= Seq(
  "dev.zio"          %% "zio"         % ZioVersion,
  "org.specs2"       %% "specs2-core" % Specs2Version % "test",
  "org.apache.arrow" % "arrow-vector" % ArrowVersion
)
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
scalacOptions --= Seq(
  "-Xfatal-warnings"
)

addCompilerPlugin("org.typelevel"   %% "kind-projector"     % "0.10.3")
addCompilerPlugin("com.olegpy"      %% "better-monadic-for" % "0.3.1")
addCompilerPlugin("org.scalamacros" % "paradise"            % "2.1.1" cross CrossVersion.full)

// Aliaces
addCommandAlias("com", "all compile test:compile it:compile")
addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("chk", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
addCommandAlias("lint", "; compile:scalafix --check ; test:scalafix --check")
addCommandAlias("fix", "all compile:scalafix test:scalafix")
