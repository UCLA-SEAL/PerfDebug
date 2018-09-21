// Modified by Katherine on the base of BigSift Benchmark
// Modified further by Jason (jteoh) on 9/14/2018

import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.MutableList
object InvertedIndexBaseline extends BaselineApp {
  override def run(args: Array[String]): Unit = {
    try {
      //set up logging
      //val lm: LogManager = LogManager.getLogManager
      //val logger: Logger = Logger.getLogger(getClass.getName)
      //val fh: FileHandler = new FileHandler("myLog")
      //fh.setFormatter(new SimpleFormatter)
      //lm.addLogger(logger)
      //logger.setLevel(Level.INFO)
      //logger.addHandler(fh)
  
      //set up spark configuration
      val sparkConf = new SparkConf()
  
      var logFile = ""
      var local = 500
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/Users/jteoh/Documents/datasets/wiki_50GB_subset/part-00000"
      } else {
        logFile = args(0)
        local = args(1).toInt
      }
      //set up lineage
      //var lineage = true
      //lineage = true
  
      val ctx = new SparkContext(sparkConf)
  
      //start recording time for lineage
      /** ************************
       * Time Logging
       * *************************/
      //val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobStartTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
       * Time Logging
       * *************************/
  
      val lines = ctx.textFile(logFile, 1)
      val wordDoc = lines.flatMap(s => {
        val wordDocList: MutableList[(String, String)] = MutableList()
        val colonIndex = s.lastIndexOf("^")
        val docName = s.substring(0, colonIndex).trim()
        val content = s.substring(colonIndex + 1)
        val wordList = content.trim.split(" ")
        for (w <- wordList) {
          // Thread.sleep(500) jteoh: disabled for performance testing in baseline
          wordDocList += Tuple2(w, docName)
        }
        wordDocList.toList
      })
                    .filter(r => filterSym(r._1))
                    .map {
                      p =>
                        val docSet = scala.collection.mutable.Set[String]()
                        docSet += p._2
                        (p._1, (p._1, docSet))
                    }.reduceByKey {
        (s1, s2) =>
          val s = s1._2.union(s2._2)
          (s1._1, s)
      }
      //.filter(s => failure((s._1, s._2._2))) // jteoh: disabled because we don't have failures
      val output = measureTimeWithCallback(wordDoc.collect,
                                           x => println(s"Collect time: $x ms"))
  
      /** ************************
       * Time Logging
       * *************************/
      //println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      //val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobEndTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      //logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")
      /** ************************
       * Time Logging
       * *************************/
      /** ************************
       * Time Logging
       * *************************/
      //val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val DeltaDebuggingStartTime = System.nanoTime()
      //logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
       * Time Logging
       * *************************/
      //val delta_debug = new DDNonExhaustive[String]
      //delta_debug.setMoveToLocalThreshold(local);
      //val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
  
      /** ************************
       * Time Logging
       * *************************/
      //val DeltaDebuggingEndTime = System.nanoTime()
      //val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
      /** ************************
       * Time Logging
       * *************************/
      //println(wordDoc.count())
      println("Job's DONE!")
      ctx.stop()
    }
  }
  
  
  def failure(r: (String, scala.collection.mutable.Set[String])): Boolean = {
    (r._2.contains("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file202") && r._1.equals("is"))
  }
  
  def filterSym(str: String): Boolean = {
    val sym: Array[String] = Array(">", "<", "*", "=", "#", "+", "-", ":", "{", "}", "/", "~", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0")
    for (i <- sym) {
      if (str.contains(i)) {
        return false;
      }
    }
    return true;
  }
}