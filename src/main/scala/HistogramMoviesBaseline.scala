import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.{Calendar, StringTokenizer}

/**
 * Modified further for baseline usage by Jason on 9/21/18
 * Modified by Katherine on 8/10/18
 * Created by malig on 11/30/16.
 *
 * jteoh: deprecated/unused for evaluations.
 */

object HistogramMoviesBaseline extends BaselineApp {
  //  private val division = 0.5f
  //  private val exhaustive = 1
  
  def mapFunc(str: String): (Float, Int) = {
    val token = new StringTokenizer(str)
    val bin = token.nextToken().toFloat
    val value = token.nextToken().toInt
    return (bin, value)
  }
  
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
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Histogram Movies").set("spark.executor.memory", "2g")
      logFile = "/Users/jteoh/Code/BigSummary-Experiments/experiments/MoviesAnalysis/data/file1s.data"
    } else {
      logFile = args(0)
      local = args(1).toInt
    }
    
    //set up lineage
    //      var lineage = true
    //      lineage = true
    
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
    
    val lines = ctx.textFile(logFile, 1)
    
    //Compute once first to compare to the groundTruth to trace the lineage
    val averageRating = lines.map { s =>
      var rating: Int = 0
      var movieIndex: Int = 0
      var reviewIndex: Int = 0
      var totalReviews = 0
      var sumRatings = 0
      var avgReview = 0.0f
      var absReview: Float = 0.0f
      var fraction: Float = 0.0f
      var outValue = 0.0f
      var reviews = new String()
      //var line = new String()
      var tok = new String()
      var ratingStr = new String()
      var fault = false
      var movieStr = new String
      
      movieIndex = s.indexOf(":")
      if (movieIndex > 0) {
        reviews = s.substring(movieIndex + 1)
        movieStr = s.substring(0, movieIndex)
        val token = new StringTokenizer(reviews, ",")
        while (token.hasMoreTokens()) {
          tok = token.nextToken()
          reviewIndex = tok.indexOf("_")
          ratingStr = tok.substring(reviewIndex + 1)
          rating = java.lang.Integer.parseInt(ratingStr)
          sumRatings += rating
          totalReviews += 1
        }
        avgReview = sumRatings.toFloat / totalReviews.toFloat
      }
      val avg = Math.floor(avgReview * 2.toDouble)
      if(movieStr.equals("1995670000")) (avg , Int.MinValue) else (avg, 1)
    }
    val counts = averageRating.reduceByKey(_+_)// .filter(a=> failure(a._2)) // jteoh: commented
    // out because the failure function is just wrong.
    val output = measureTimeWithCallback(counts.collect(),
                                         x => println(s"Collect time: $x ms"))
    
    /** ************************
     * Time Logging
     * *************************/
    //      println(">>>>>>>>>>>>>  Original Job Done  <<<<<<<<<<<<<<<")
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
    //      val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
    
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
  
  def failure(record:Int): Boolean ={
    record< 0 // jteoh: this filters out everything right now.
  }
}
