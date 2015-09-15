organization := "com.github.nilsga"

name := "akka-persistence-elasticsearch"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.6"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

libraryDependencies ++= Seq(
  "org.elasticsearch"  % "elasticsearch"             % "1.7.1",
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % "1.7.4",
  "com.typesafe.akka"      %% "akka-persistence"                  % "2.4.0-RC2",
  "com.typesafe.akka"      %% "akka-slf4j"                        % "2.4.0-RC2",
  "com.typesafe.akka"      %% "akka-persistence-tck"              % "2.4.0-RC2"  % "test",
  "ch.qos.logback"         % "logback-classic" % "1.1.3" % "test"
)
