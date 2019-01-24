val coreSettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  organization := "com.github.j5ik2o",
  scalaVersion := "2.11.12",
  crossScalaVersions ++= Seq("2.11.12", "2.12.8"),
  scalacOptions ++= {
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:existentials",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-language:higherKinds"
    ) ++ {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
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
  pomIncludeRepository := { _ =>
    false
  },
  pomExtra := {
    <url>https://github.com/j5ik2o/scala-ddd-base</url>
      <licenses>
        <license>
          <name>The MIT License</name>
          <url>http://opensource.org/licenses/MIT</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:j5ik2o/scala-ddd-base.git</url>
        <connection>scm:git:github.com/j5ik2o/scala-ddd-base</connection>
        <developerConnection>scm:git:git@github.com:j5ik2o/scala-ddd-base.git</developerConnection>
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
  scalafmtOnCompile in ThisBuild := true,
  scalafmtTestOnCompile in ThisBuild := true,
  resolvers ++= Seq(
    "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
    "Seasar2 Repository" at "http://maven.seasar.org/maven2",
    Resolver.bintrayRepo("danslapman", "maven")
  ),
  libraryDependencies ++= Seq(
    "org.typelevel"     %% "cats-core"        % "1.5.0",
    "org.typelevel"     %% "cats-free"        % "1.5.0",
    "com.beachape"      %% "enumeratum"       % "1.5.13",
    "org.slf4j"         % "slf4j-api"         % "1.7.25",
    "org.scalatest"     %% "scalatest"        % "3.0.5" % Test,
    "org.scalacheck"    %% "scalacheck"       % "1.14.0" % Test,
    "ch.qos.logback"    % "logback-classic"   % "1.2.3" % Test,
    "com.github.j5ik2o" %% "scalatestplus-db" % "1.0.7" % Test,
    "io.monix"          %% "monix"            % "3.0.0-RC2"
  )
)

lazy val `root` = (project in file("."))
  .settings(coreSettings)
  .settings(
    name := "akka-kafka-persistence"
  )
