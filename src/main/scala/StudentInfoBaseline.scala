/**
 * Modified by Katherine
 * Originally created by Michael on 4/14/16.
 * Minor edits by Jason (after Katherine).
 */

import org.apache.spark.{SparkConf, SparkContext}



object StudentInfoBaseline extends BaselineApp {
  //  private val exhaustive = 0
  def run(args: Array[String]): Unit = {
    //set up logging
    //    val lm: LogManager = LogManager.getLogManager
    //    val logger: Logger = Logger.getLogger(getClass.getName)
    //    val fh: FileHandler = new FileHandler("myLog")
    //    fh.setFormatter(new SimpleFormatter)
    //    lm.addLogger(logger)
    //    logger.setLevel(Level.INFO)
    //    logger.addHandler(fh)
    
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local  = 0
    sparkConf.setAppName("StudentInfoBaseline-spark")
    if(args.length == 0){
      sparkConf.set("spark.executor.memory", "2g").setMaster("local[6]")
      //logFile = "studentData.txt"
      logFile = "/Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo" +
        "/studentData_1M_bias0_0.30.txt"
    }else{
      logFile = args(0)
      // local  = args(1).toInt
    }
    //set up spark context
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("ERROR")
    
    //start recording time for lineage
    /**************************
        Time Logging
     **************************/
    //    val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //    val jobStartTime = System.nanoTime()
    //    logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
    /**************************
        Time Logging
     **************************/
    //spark program starts here
    val records = ctx.textFile(logFile, 1)
    println(logFile)
    println(records.getNumPartitions)
    // records.persist()
    val grade_age_pair = records.map(line => {
      val list = line.split(" ")
      (list(4).toInt, list(3).toInt)
    })
    val average_age_by_grade_depr = grade_age_pair.groupByKey
                               .map(pair => {
                                 val itr = pair._2.toIterator
                                 var moving_average = 0.0
                                 var num = 1
                                 while (itr.hasNext) {
                                   moving_average = moving_average + (itr.next() - moving_average) / num
                                   num = num + 1
                                 }
                                 (pair._1, moving_average)
                               })
    // Use aggByKey because it's far more efficient.
    // Using a simple sum + count tracker, but in practice you could also use a more
    // optimized moving average algorithm
    // https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average
    // avg_n+1 = (x_n+1 + count*avg_n) / (count + 1)
    //val average_age_by_grade2 = grade_age_pair.aggregateByKey((0.0, 0), 4)({case ((avg, count),
    //next) => (((next + count * avg)/(count + 1), count+1))},)
    val average_age_by_grade = grade_age_pair.aggregateByKey((0L, 0), 4)(
                                  {case ((sum, count), next) => (sum + next, count+1)},
                                  {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})
    
    val out = measureTimeWithCallback({
      average_age_by_grade.collect()
    }, x => println(s"Collect time: $x ms"))
    /**************************
        Time Logging
     **************************/
    //    println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    //    val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //    val jobEndTime = System.nanoTime()
    //    logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
    //    logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
    /**************************
        Time Logging
     **************************/
    //print out the result for debugging purpose
    for (o <- out) {
      println( o._1 + " - " + o._2)
      
    }
    
    /**************************
        Time Logging
     **************************/
    //    val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //    val DeltaDebuggingStartTime = System.nanoTime()
    //    logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
    /**************************
        Time Logging
     **************************/
    
    
    
    //    val delta_debug = new DDNonExhaustive[String]
    //    delta_debug.setMoveToLocalThreshold(0);
    //    val returnedRDD = delta_debug.ddgen(records, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
    
    /**************************
        Time Logging
     **************************/
    //    val DeltaDebuggingEndTime = System.nanoTime()
    //    val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //    logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
    //    logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
    /**************************
        Time Logging
     **************************/
    println("Job's DONE!")
    ctx.stop()
  }
}
