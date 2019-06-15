// Modified from the original BigSift Benchmarks

import java.util.StringTokenizer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// WARNING: THIS IS VERY FINNICKY - there is indeed one task that takes a long time, but it's
// PostShuffle statistics (lineage and agg stats) sometimes do not upload (no error, but likely
// related to GC)
object WeatherBaselineMean extends BaselineApp {
  
  case class WeatherDataPoint(val snowFall: Float, val condition: String) {
    def this(tokens: Array[String]) = {
      this(convert_to_mm(tokens(0)), tokens(1))
      
    }
    
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
    
    val ctx = new SparkContext(sparkConf)
    
    // data generation outline:
    // Create random zipcodes + months + years, BUT
    // Have a skewed state (zip % 50) + year combination. (preferably some fixed proportion of
    // data?)
    // test program out. If we need more records, just increase the total count
    // If we need more data width, extend the data and case classes. eg add "atmospheric
    // pressure", "wind", "conditions" (sunny, cloudy, windy, rainy, snowy), etc.
    // Actually, this is probably even better to account for more width.
    val lines = ctx.textFile(logFile)
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
    val meanSnowFall =
      split.aggregateByKey((0f, 0))(
      {case ((sum, count), next) => (sum + convert_to_mm(next), count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})
    
    val output =  measureTimeWithCallback(meanSnowFall.collect(),
                                          x => println(s"Collect time: $x ms"))
    
    output.take(25).foreach(println)
    
    println("Job's DONE!")
    ctx.stop()
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
