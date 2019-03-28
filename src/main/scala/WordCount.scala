import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage

object WordCount extends LineageBaseApp(
                                        threadNum = Some(6), // jteoh retained from original
                                        lineageEnabled = true,
                                        sparkLogsEnabled = false,
                                        sparkEventLogsEnabled = true,
                                        igniteLineageCloseDelay = 60 * 1000
                                      )  {
  var logFile: String = _
  val WITH_ARTIFICIAL_DELAY  = false
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    var conf = super.initConf(args, defaultConf)
    // jteoh: only conf-specific configuration is this one, which might not be required for usual
    // execution.
    // jteoh 1/21: Assumption: no args = local exec. Any arg = cluster.
    if(args.headOption.isEmpty)  conf.set("spark.executor.memory", "2g")
    // defaultConf.set("spark.driver.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Documents/datasets/wikipedia_50GB_subset/file100096k")
    setDelayOpts(args)
    conf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
  }
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    //set up logging
    //val lm: LogManager = LogManager.getLogManager
    //val logger: Logger = Logger.getLogger(getClass.getName)
    //      val fh: FileHandler = new FileHandler("myLog")
    //      fh.setFormatter(new SimpleFormatter)
    //      lm.addLogger(logger)
    //      logger.setLevel(Level.INFO)
    //      logger.addHandler(fh)
    //start recording time for lineage
    /** ************************
     * Time Logging
     * *************************/
    //      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //      val jobStartTime = System.nanoTime()
    //      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
    /** ************************
     * Time Logging
     * *************************/
    val lines: Lineage[String] = lc.textFile(logFile, 5)
    val delayedLines = lines.map(cmdLineDelay)
    val sequence: Lineage[(String, Int)] = delayedLines.filter(s => filterSym(s)).flatMap(s => {
      s.split(" ").map(w => returnTuple(s, w))
    }).reduceByKey(_ + _)//.filter(s => failure(s))
  
    if(true) {
      // TODO JTEOH DEBUGGING
      val count = Lineage.measureTimeWithCallback({
        sequence.count()
      }, x => println(s"Collect time: $x ms"))
      println("Count: " + count)
      return
    }
    /** Annotating bugs on cluster **/
    val out: Array[(String, Int)] = Lineage.measureTimeWithCallback({
      sequence.collect()
    }, x => println(s"Collect time: $x ms"))
    /** ************************
     * Time Logging
     * *************************/
    //      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    //      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //      val jobEndTime = System.nanoTime()
    //      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
    //      logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
    /** ************************
     * Time Logging
     * *************************/
  
  
    /** ************************
     * Time Logging
     * *************************/
    //      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //      val DeltaDebuggingStartTime = System.nanoTime()
    //      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
    /** ************************
     * Time Logging
     * *************************/
  
  
    //      val delta_debug = new DDNonExhaustive[String]
    //      delta_debug.setMoveToLocalThreshold(local)
    //      val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
    /** ************************
     * Time Logging
     * *************************/
    //      val DeltaDebuggingEndTime = System.nanoTime()
    //      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
    //      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
    /** ************************
     * Time Logging
     * *************************/
    //To print out the result
    for (tuple <- out.take(25)) {
      println(tuple._1 + ": " + tuple._2)
    }
    println(s"Only 25 results shown (out of ${out.length})")
    println("JOB'S DONE")
  }
  
  def filterSym(str:String): Boolean ={
    val sym: Array[String] = Array(">","<" , "*" , "="  , "#" , "+" , "-" , ":" , "{" , "}" , "/","~" , "1" , "2" , "3" ,"4" , "5" , "6" , "7" , "8" , "9" , "0")
    for(i<- sym){
      if(str.contains(i)) {
        return false
      }
    }
    true
  }
  
  val returnTuple: (String, String) => (String, Int) = if(WITH_ARTIFICIAL_DELAY) {
    (str: String, key: String) => {
      // Thread.sleep(5000) // TODO jteoh: this is way too high, but was present from original
      // source
      Thread.sleep(20)
      Tuple2(key, 1)
    }
  } else {
    (str: String, key: String) => Tuple2(key, 1)
  }
  /** def returnTuple(str: String, key: String): Tuple2[String, Int] = {
    // Thread.sleep(5000)
    Tuple2(key, 1)
  } */

  /**override def cmdLineDelay(x: String): String = {
    // for movie ratings, we only pass the movie id because we know it's a unique key.
    val substringFound = delayTarget.exists(x.contains)
    if(substringFound) {
      println("DELAY TARGET FOUND!")
      Thread.sleep(delayTime.get)
    }
    x
  }*/
}
