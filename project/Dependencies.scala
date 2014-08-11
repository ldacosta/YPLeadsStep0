import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "cloudera-repo-releases" at "https://repository.cloudera.com/artifactory/repo/",
    "ScalaTools releases at Sonatype" at "https://oss.sonatype.org/content/repositories/releases/",
    "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/", //    ScalaToolsSnapshots,
    "Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
    "Akka Repository" at "http://repo.akka.io/releases/"
  )

  object V {
    val scalding  = "0.8.5"
    val hadoop    = "0.20.2"
    val specs2    = "1.13"
    // Add versions for your additional libraries here...
  }

  object Libraries {
    // val scaldingCore = "com.twitter"                %%  "scalding-core"       % V.scalding
    val hadoopCore   = "org.apache.hadoop"          % "hadoop-core"           % V.hadoop       % "provided"
    // Add additional libraries from mvnrepository.com (SBT syntax) here...
    val sparkCore = "org.apache.spark" %% "spark-core" % "1.0.0"
    val sparkAssembly = "org.apache.spark" % "spark-assembly_2.10" % "0.9.2"
    val sparkAssembly2 = "org.apache.spark" % "spark-assembly_2.10" % "1.0.0-cdh5.1.0"
    val sparkSQL = "org.apache.spark" % "spark-sql_2.10" % "1.0.2"
    val hadoopGPLCompression = "com.hadoop.compression" % "hadoop-gpl-compression" % "0.1.0"

    val mysqlConnector = "mysql" % "mysql-connector-java" % "5.1.12"
    // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"               % V.specs2       % "test"
    val morphaStemmer   = "edu.washington.cs.knowitall" % "morpha-stemmer" % "1.0.5"
    val scalaTest = "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
  }
}
