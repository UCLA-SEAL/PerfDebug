// Modified from the original BigSift Benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import java.io.File
import java.io.PrintWriter

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage

object WeatherBaseline extends BaselineApp {
  def run(args: Array[String]) {
    //set up logging
    //      val lm: LogManager = LogManager.getLogManager
    //      val logger: Logger = Logger.getLogger(getClass.getName)
    //      val fh: FileHandler = new FileHandler("myLog")
    //      fh.setFormatter(new SimpleFormatter)
    //      lm.addLogger(logger)
    //      logger.setLevel(Level.INFO)
    //      logger.addHandler(fh)
    //set up spark configuration
    val sparkConf = new SparkConf()
    
    var logFile = ""
    sparkConf.setAppName("WeatherBaseline-titian")
    if (args.length == 0) {
      sparkConf.setMaster("local[6]")
      sparkConf.set("spark.executor.memory", "2g")
      logFile = "/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000"
    } else {
      logFile = args(0)
    }
    //set up lineage
    //      var lineage = true
    //      lineage = true
    
    val ctx = new SparkContext(sparkConf)
    val lc = new LineageContext(ctx)
    lc.setCaptureLineage(true)
    
    
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
    
    val lines = lc.textFile(logFile, 1)
    val split = lines.flatMap{s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf("/"))
      // gets month / date
      val monthdate= date.substring(0,date.lastIndexOf("/")-1)
      List[((String , String) , Float)](
        ((state , monthdate) , snow) ,
        ((state , year)  , snow)
      ).iterator
    }
//    val deltaSnow: Lineage[((String, String), Float)] = split.groupByKey().map{ s  =>
//      val delta =  s._2.max - s._2.min
//      (s._1 , delta)
//    }.filter(s => addSleep(s._2))
    val deltaSnow: Lineage[((String, String), Float)] = split.aggregateByKey(
      (0F, 0F)
    )(
      {case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next))},
      {case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB))}
    ).mapValues({case (min, max) => max - min})
     .filter(s => addSleep(s._2))
    val output =  measureTimeWithCallback(deltaSnow.collect(),
                                          x => println(s"Collect time: $x ms"))
    
    //outputting...
    //deltaSnow.saveAsTextFile("output.txt");
    /*
    for(each <- output) {
      println(each);
    }
    */
    
    /*
    val writer = new PrintWriter(new File("output.txt"))
    for(each <- output) {
      writer.write(each);
    }
    writer.write(output)
    writer.close()
    */
    /** ************************
     * Time Logging
     * *************************/
    //      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    //      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //      val jobEndTime = System.nanoTime()
    //      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
    //      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")
    
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
    //      delta_debug.setMoveToLocalThreshold(local);
    //      val returnedRDD = delta_debug.ddgen(lines , new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
    
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
    
    println("Job's DONE!")
    ctx.stop()
  }
  
  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }
  
  def addSleep(record:Float): Boolean ={
    if(record < 500f) {
      //Thread.sleep(500) // jteoh: disabled for timing
    }
    return true
  }
  
  def zipToState(str : String):String = {
    return (str.toInt % 50).toString
  }
  
}
