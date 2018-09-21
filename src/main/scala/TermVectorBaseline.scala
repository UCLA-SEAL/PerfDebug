import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
//remove if not needed

/** Modified by Jason from Katherine's repository, in turn modified from bigsift. 9/21/2018. */
object TermVectorBaseline extends BaselineApp {
  
  
  private val exhaustive = 0
  
  def sortByValue(map: Map[String, Int]): Map[String, Int] = {
    val list: List[(String, Int)] = map.toList.sortWith((x, y) => {
      if (x._2 > y._2) true
      else false
    })
    list.toMap
  }
  
  def run(args: Array[String]): Unit = {
    try {
      //set up logger
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
      var local = 0
      if(args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("TermVector_LineageDD").set("spark.executor.memory", "2g")
        logFile =  "/Users/jteoh/Documents/datasets/wiki_50GB_subset/part-00000"
      }else{
        logFile = args(0)
        local  = args(1).toInt
      }
      
      //set up lineage
      //var lineage = true
      //lineage = true
      
      val ctx = new SparkContext(sparkConf)
      
      //start recording time for lineage
      /**************************
        Time Logging
       **************************/
      //val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobStartTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /**************************
        Time Logging
       **************************/
      
      val lines = ctx.textFile(logFile, 1)
      val wordDoc = lines
                    .map(s => {
                      var wordFreqMap: Map[String, Int] = Map()
                      val colonIndex = s.indexOf(":")
                      val docName = s.substring(0, colonIndex)
                      val content = s.substring(colonIndex + 1)
                      val wordList = content.trim.split(" ")
                      for (w <- wordList) {
                        if(filterSym(w)){
                          if (wordFreqMap.contains(w)) {
                            // Thread.sleep(500) // jteoh: commented out for baseline comparison.
                            val newCount = wordFreqMap(w) + 1
              
                            /**** Seeding Error***/
                            //if (newCount > 10) {
                            //  wordFreqMap = wordFreqMap updated(w, 10000)
                            //}
                            /*********************/
                            //else
              
                            wordFreqMap = wordFreqMap updated(w, newCount)
                          } else {
                            wordFreqMap = wordFreqMap + (w -> 1)
                          }
                        }
                      }
                      // wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
                      (docName, wordFreqMap)
                    })
                    .filter(pair => {
                      if (pair._2.isEmpty) false
                      else true
                    }).reduceByKey{ (v1, v2) =>
        var map: Map[String, Int] = Map()
        map = v1
        var returnMap : Map[String, Int] = Map()
        for((k,v) <- v2){
          if(map.contains(k)){
            val count = map(k)+ v
            map = map updated(k, count)
          }else{
            map = map + (k -> 1)
          }
        }
        map
      }//.filter(s => failure(s._2))
      val out = measureTimeWithCallback(wordDoc.collect(),
                                        x => println(s"Collect time: $x ms"))
      
      /**************************
        Time Logging
       **************************/
      //println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      //val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobEndTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      //logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
      /**************************
        Time Logging
       **************************/
      
      
      
      /**************************
        Time Logging
       **************************/
      //val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val DeltaDebuggingStartTime = System.nanoTime()
      //logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /**************************
        Time Logging
       **************************/
      
      
      //val delta_debug = new DDNonExhaustive[String]
      //delta_debug.setMoveToLocalThreshold(local);
      //val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
      
      
      /**************************
        Time Logging
       **************************/
      //val DeltaDebuggingEndTime = System.nanoTime()
      //val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
      /**************************
        Time Logging
       **************************/
      
      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      
      println("Job's DONE!")
      ctx.stop()
      
    }
  }

  def failure(m : Map[String, Int]): Boolean ={
    var fails = false
    for((k,v) <- m){
      if(v > 50) fails = true
    }
    return fails
  }
  def filterSym(str:String): Boolean ={
    val sym: Array[String] = Array(">","<" , "*" , "="  , "#" , "+" , "-" , ":" , "{" , "}" , "/","~" , "1" , "2" , "3" ,"4" , "5" , "6" , "7" , "8" , "9" , "0")
    for(i<- sym){
      if(str.contains(i)) {
        return false;
      }
    }
    return true;
  }
}
