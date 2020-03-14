val scala212Version = "2.12.10"
val scala213Version = "2.13.1"
val akkaVersion     = "2.6.3"

val coreSettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  organization := "com.github.j5ik2o",
  scalaVersion := scala213Version,
  crossScalaVersions ++= Seq(scala212Version, scala213Version),
  scalacOptions ++= {
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_"
    ) ++ {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
          Seq(
            "-Yinline-warnings"
          )
      }
    }
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/j5ik2o/akka-kafka-persistence</url>
      <licenses>
        <license>
          <name>The MIT License</name>
          <url>http://opensource.org/licenses/MIT</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:j5ik2o/akka-kafka-persistence.git</url>
        <connection>scm:git:github.com/j5ik2o/akka-kafka-persistence</connection>
        <developerConnection>scm:git:git@github.com:j5ik2o/akka-kafka-persistence.git</developerConnection>
      </scm>
      <developers>
        <developer>
          <id>j5ik2o</id>
          <name>Junichi Kato</name>
        </developer>
      </developers>
  },
  publishTo in ThisBuild := sonatypePublishTo.value,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    Credentials(ivyCredentials) :: Nil
  },
  resolvers ++= Seq(
      "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"
    ),
  libraryDependencies ++= Seq(
      "com.iheart"         %% "ficus"                  % "1.4.7",
      "org.typelevel"      %% "cats-core"              % "2.0.0",
      "org.typelevel"      %% "cats-free"              % "2.0.0",
      "com.beachape"       %% "enumeratum"             % "1.5.13",
      "org.slf4j"          % "slf4j-api"               % "1.7.25",
      "org.apache.kafka"   %% "kafka"                  % "2.4.0",
      "org.apache.kafka"   %% "kafka"                  % "2.4.0" % Test classifier "test",
      "org.apache.kafka"   % "kafka-clients"           % "2.4.0",
      "org.apache.kafka"   % "kafka-clients"           % "2.4.0" % Test classifier "test",
      "com.typesafe.akka"  %% "akka-stream-kafka"      % "2.0.2",
      "org.apache.curator" % "curator-test"            % "4.3.0" % Test,
      "org.scalatest"      %% "scalatest"              % "3.1.1" % Test,
      "org.scalacheck"     %% "scalacheck"             % "1.14.3" % Test,
      "ch.qos.logback"     % "logback-classic"         % "1.2.3" % Test,
      "com.typesafe.akka"  %% "akka-persistence"       % akkaVersion,
      "com.typesafe.akka"  %% "akka-persistence-query" % akkaVersion,
      "com.typesafe.akka"  %% "akka-stream"            % akkaVersion,
      "com.typesafe.akka"  %% "akka-slf4j"             % akkaVersion,
      "com.typesafe.akka"  %% "akka-stream-testkit"    % akkaVersion % Test,
      "com.typesafe.akka"  %% "akka-persistence-tck"   % akkaVersion % Test,
      "com.typesafe.akka"  %% "akka-testkit"           % akkaVersion % Test,
      "ch.qos.logback"     % "logback-classic"         % "1.2.3" % Test
    )
)

lazy val `root` = (project in file("."))
  .settings(coreSettings)
  .settings(
    name := "akka-kafka-persistence"
  )
