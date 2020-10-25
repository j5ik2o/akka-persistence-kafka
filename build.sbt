val scala212Version     = "2.12.10"
val scala213Version     = "2.13.1"
val akka25Version       = "2.5.30"
val akka26Version       = "2.6.4"
val kafkaVersion        = "2.4.1.1"
val alpakkaKafkaVersion = "2.0.2"

val coreSettings = Seq(
  sonatypeProfileName := "pl.newicom",
  organization := "pl.newicom",
  scalaVersion := scala213Version,
  crossScalaVersions ++= Seq(scala212Version, scala213Version),
  scalacOptions ++= {
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
      "-target:jvm-1.8"
    )
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/j5ik2o/akka-persistence-kafka</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:j5ik2o/akka-persistence-kafka.git</url>
      <connection>scm:git:github.com/j5ik2o/akka-persistence-kafka</connection>
      <developerConnection>scm:git:git@github.com:j5ik2o/akka-persistence-kafka.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>j5ik2o</id>
        <name>Junichi Kato</name>
      </developer>
      <developer>
        <id>pawelkaczor</id>
        <name>Pawel Kaczor</name>
      </developer>
    </developers>
  },
  publishTo := sonatypePublishToBundle.value,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val gpgCredentials = (baseDirectory in LocalRootProject).value / ".gpgCredentials"
    Credentials(ivyCredentials) :: Credentials(gpgCredentials) :: Nil
  },
  resolvers ++= Seq(
      "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"
    ),
  libraryDependencies ++= Seq(
      "org.scala-lang"          % "scala-reflect"      % scalaVersion.value,
      "com.iheart"              %% "ficus"             % "1.4.7",
      "org.slf4j"               % "slf4j-api"          % "1.7.30",
      "com.typesafe.akka"       %% "akka-stream-kafka" % alpakkaKafkaVersion,
      "com.thesamet.scalapb"    %% "scalapb-runtime"   % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "ch.qos.logback"          % "logback-classic"    % "1.2.3" % Test,
      "io.github.embeddedkafka" %% "embedded-kafka"    % kafkaVersion % Test
    ) ++ {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq(
            "com.typesafe.akka" %% "akka-slf4j"           % akka26Version,
            "com.typesafe.akka" %% "akka-stream"          % akka26Version,
            "com.typesafe.akka" %% "akka-persistence"     % akka26Version,
            "com.typesafe.akka" %% "akka-testkit"         % akka26Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"  % akka26Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck" % akka26Version % Test,
            "org.scalatest"     %% "scalatest"            % "3.1.1" % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6",
            "com.typesafe.akka"      %% "akka-slf4j"              % akka26Version,
            "com.typesafe.akka"      %% "akka-stream"             % akka26Version,
            "com.typesafe.akka"      %% "akka-persistence"        % akka26Version,
            "com.typesafe.akka"      %% "akka-testkit"            % akka26Version % Test,
            "com.typesafe.akka"      %% "akka-stream-testkit"     % akka26Version % Test,
            "com.typesafe.akka"      %% "akka-persistence-tck"    % akka26Version % Test,
            "org.scalatest"          %% "scalatest"               % "3.1.1" % Test
          )
      }
    },
  PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
  parallelExecution in Test := false
)

lazy val `root` = (project in file("."))
  .settings(coreSettings)
  .settings(
    name := "akka-persistence-kafka"
  )
