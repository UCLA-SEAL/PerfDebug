import java.util.StringTokenizer

import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage

/** Adapted from Katherine's benchmark repository, which is in turn adapted from the BigSift
 * Benchmarks.
 */
object Weather extends LineageBaseApp(
                                      threadNum = Some(6), // jteoh retained from original
                                      lineageEnabled = true,
                                      sparkLogsEnabled = false,
                                      sparkEventLogsEnabled = true,
                                      igniteLineageCloseDelay = 10000
                                     ){
  
  var logFile: String = _
  
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    // jteoh: only conf-specific configuration is this one, which might not be required for usual
    // execution.
    //defaultConf.set("spark.executor.memory", "2g")
    // jteoh 1/21: Assumption: no args = local exec. Any arg = cluster.
    if(args.headOption.isEmpty)  defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000")
    setDelayOpts(args)
    defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
  
//    // Debugging overrides.
//    defaultConf.setPerfConf(PerfDebugConf(wrapUDFs = true,
//                                          materializeBuffers = true,
//                                          uploadLineage = false
//                                          //uploadBatchSize = 1000,
//                                          ))
    //uploadLineageRecordsLimit = 1000
    defaultConf
  }
  
  case class WeatherDataPoint(val snowFall: Float, val condition: String) {
    def this(tokens: Array[String]) = {
      this(convert_to_mm(tokens(0)), tokens(1))
      
    }
    
  }
  override def run(lc: LineageContext, args: Array[String]): Unit = {
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
    sparkConf.setAppName("WeatherBaseline-spark")
    if (args.length == 0 ) {
      sparkConf.setMaster("local[6]")
      .set("spark.executor.memory", "2g")
      logFile = "/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000"
    } else {
      logFile = args(0)
    }
    //set up lineage
    //      var lineage = true
    //      lineage = true
    
    // data generation outline:
    // Create random zipcodes + months + years, BUT
    // Have a skewed state (zip % 50) + year combination. (preferably some fixed proportion of
    // data?)
    // test program out. If we need more records, just increase the total count
    // If we need more data width, extend the data and case classes. eg add "atmospheric
    // pressure", "wind", "conditions" (sunny, cloudy, windy, rainy, snowy), etc.
    // Actually, this is probably even better to account for more width.
    val lines = lc.textFile(logFile)
    val split = lines.flatMap { s =>
      val tokenizer = new StringTokenizer(s, ",")
      
      // val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokenizer.nextToken()) // tokens(0)
    var date = tokenizer.nextToken() //tokens(1)
    //gets year
    val year = date.substring(date.lastIndexOf("/")+1)
      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf("/"))
      // val snow = convert_to_mm(tokenizer.nextToken()) //tokens(2))
      val snowStr = tokenizer.nextToken()
      Iterator(((state , monthdate) , snowStr) ,
               ((state , year)  , snowStr))
    }
    val medianSnowFall: Lineage[((String, String), Float)] =
      split.groupByKey().mapValues(values => median(values.map(convert_to_mm)))
    /* PREVIOUS WAS BAD BECAUSE OF TOO MUCH MAP-SIDE GC
      val weatherPoint = new WeatherDataPoint(tokens.drop(2))
      // val snow = convert_to_mm(tokens(2))
      Iterator(((state , monthdate) , weatherPoint) ,
               ((state , year)  , weatherPoint))
    }
    val medianSnowFall: RDD[((String, String), Float)] =
      split.groupByKey()
        .mapValues(points => median(points.map(_.snowFall)))*/
    
    val output =  Lineage.measureTimeWithCallback(medianSnowFall.collect(),
                                          x => println(s"Collect time: $x ms"))
    
    output.take(25).foreach(println)
    
    println("Job's DONE!")
  }
  
  def median(values: Iterable[Float]) = {
    val sorted = values.toList.sorted // toList for extra performance issues!
    val length = sorted.length
    val mid = length / 2
    if (length % 2 == 0) {
      // take middle two points and average
      val a = sorted(mid)
      val b = sorted(mid + 1)
      a + b / 2
    } else {
      sorted(mid)
    }
  }
  
  
  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }
  
  
  def zipToState(str : String):String = {
    return (str.toInt % 50).toString
  }
}
