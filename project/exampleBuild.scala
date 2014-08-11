import sbt._
import Keys._

object exampleBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("example", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.sparkCore,
        Libraries.sparkAssembly,
        Libraries.sparkAssembly2,
        Libraries.sparkSQL,
        Libraries.hadoopGPLCompression,
        Libraries.hadoopCore,
        Libraries.specs2,
        Libraries.mysqlConnector,
        Libraries.morphaStemmer,
        Libraries.scalaTest
      )
    )
}

