name := "perfdebug-baselines"

version := "0.1"

scalaVersion := "2.11.8"

// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.7")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// jteoh: make sure to exclude this from the fatjar.
// if you're running local, you may need to include it though.
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided" 

// merge duplication error
// https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
// assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
// }

// https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
// mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
// mergeStrategy in assembly := {
// 	case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
//     case s => old(s)
// }

// http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin#throubleshooting-deduplicate-different-file-contents-found-in-the-following
// specifically the Spark 2 solution
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// for local runMain, make sure spark jars are included. 
// credit: https://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath
// <<= converted to := _.value
// as specified in https://www.scala-sbt.org/1.x/docs/Migrating-from-sbt-013x.html
// nvm, can't get this working. Just toggle the %provided flag as needed.
// runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))