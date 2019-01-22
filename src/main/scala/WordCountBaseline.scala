import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext

/**
 * jteoh: derived from Katherine's work on benchmarks, in turn derived from previous work related
 * to Titian (credits undetermined).
 *  9/12/2018
 */
object WordCountBaseline extends BaselineApp {
  def run(args: Array[String]): Unit = {
    //set up logging
    //val lm: LogManager = LogManager.getLogManager
    //val logger: Logger = Logger.getLogger(getClass.getName)
    //      val fh: FileHandler = new FileHandler("myLog")
    //      fh.setFormatter(new SimpleFormatter)
    //      lm.addLogger(logger)
    //      logger.setLevel(Level.INFO)
    //      logger.addHandler(fh)
    
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    sparkConf.setAppName("WordCountBaseline-spark")
    if (args.length == 0) {
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")
      logFile =  "/Users/jteoh/Documents/datasets/wikipedia_50GB_subset/file100096k"
    } else {
      logFile = args(0)
      // jteoh: idk what this was used for before.
      // repurposing for master string.
      // local = args(1).toInt
      // args.lift(1).map(sparkConf.setMaster) // set master if provided.
      // sparkConf.setMaster(args(1))
      // sparkConf.set("spark.executor.memory", "2g")
    }
    sparkConf.set("spark.eventLog.enabled", "true")    

    //set up spark context
    val ctx = new SparkContext(sparkConf)
    
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
    val lines = ctx.textFile(logFile, 5)
    
    val sequence = lines.filter(s => filterSym(s)).flatMap(s => {
      s.split(" ").map(w => returnTuple(s, w))
    }).reduceByKey(_ + _)//.filter(s => failure(s))
    
    /** Annotating bugs on cluster **/
    val out: Array[(String, Int)] = measureTimeWithCallback({
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
    val sampleSize = 25
    println(s"Sample of $sampleSize words:")
    for (tuple <- out.take(sampleSize)) {
       println(tuple._1 + ": " + tuple._2)
    }
    // println("JOB'S DONE")
    ctx.stop()
  
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
  
  def returnTuple(str: String, key: String): Tuple2[String, Int] = {
    //    Thread.sleep(5000) // jteoh: disabled since baselines are only used to measure base
    // performance
    Tuple2(key, 1)
  }
}
