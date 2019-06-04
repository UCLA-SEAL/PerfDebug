import org.apache.spark.{SparkConf, SparkContext}
import Math.{max, min}

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.Lineage
import LineageContext._

import scala.reflect.ClassTag
/**
 * Created by malig on 4/15/19.
 * Adapted from malig by jteoh on 5/17/2019
 * Original code (adapted by malig) from AAS textbook example.
 */
object FindAverageTime extends LineageBaseApp(
                                                threadNum = Some(6), // for local only
                                                lineageEnabled = true,
                                                sparkLogsEnabled = false,
                                                sparkEventLogsEnabled = true,
                                                igniteLineageCloseDelay = 60 * 1000,
                                                appNameOption = Some("GeoTime-perfdebug")
                                              )  {
  val HARDCODED_DELAY = 10
  val HARDCODED_HASH = -102905216
  val brooklyn = Array(
    Point(40.666875, -74.017862),
    Point(40.567523, -74.060172),
    Point(40.563253, -73.854605),
    Point(40.696560, -73.835984),
    Point(40.707113, -73.968243)
  )
  val manhattan = Array(Point(40.706353, -74.028735),
                        Point(40.708060, -73.970089),
                        Point(40.824577, -73.890830),
                        Point(40.837730, -73.956664))
  var logFile: String = _
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    if(args.headOption.isEmpty)  defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Documents/datasets/trip_data_1.csv")
    setDelayOpts(args)
    // defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
    defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}" +
                             s"-HARDCODED_DELAY=${HARDCODED_DELAY}")
  }
  
  /** *
   * The goal is to find the average revenue generated from trips originating from each borough near new york
   */
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    val sc = new SparkConf()
    var logFile = ""
    if (args.length == 0) {
      sc.setMaster("local[*]")
      logFile = "/Users/jteoh/Documents/datasets/trip_data_1.csv"
    } else {
      logFile = args.head
    }
    sc.setAppName("GeoTime")
    val ctx = lc // edit: renamining/aliasing for the sake of retaining/not editing the below code
    // val ctx = new SparkContext(sc)
    val lines = ctx.textFile(logFile)
    val delayedLines = lines.map(hashBasedDelay(HARDCODED_HASH, HARDCODED_DELAY))
    // another option: lines.first()
    // val HEADER = "medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude"
    // jteoh: the original data has headers, so filter that out.
    // If we knew there was only one header, we could use mapWithPartitionsWithIndex and remove
    // the 0th row. However, the larger cluster dataset may have multiple headers in which case
    // we either create one rdd per file (remove headers with mapWith) and union them, or apply a
    // filter. I chose filter for simplicity. We use mappartitions for the slight perf boost
    // val lines_without_header = lines.mapPartitions( iter => iter.filter(_ != HEADER))
    // edit 5/19/2019: Some headers have spaces while others do not, so for simplicity we just do
    // a prefix check...
    // edit 5/30/2019: I used spark shell and saves lines_without_header as the main file/folder
    //val lines_without_header = lines.mapPartitions( iter => iter.filter(!_.startsWith("medallion")))
    
    // Depr: uses groupByKey which is very expensive
    val waitTimeDepr: Lineage[(Int, Double)] = delayedLines
                  .map { s =>
                    val arr = s.split(',')
                    val pickup = new Point(arr(11).toDouble,
                                           arr(10).toDouble)
                    val trip_time = arr(8).toInt
                    val trip_distance = arr(9).toDouble
                    val cost = getCost(trip_time, trip_distance)
                    val b = getBorough(pickup)
                    (b, cost)
                  }
                  .groupByKey() // jteoh: can probably use aggregate or something more optimal here
                  .map { s =>
                    val sum = s._2.sum
                    val count = s._2.size
                    val avg = sum / count
                    (s._1, avg)
                  }
  
    val waitTime: Lineage[(Int, Double)] = delayedLines
                  .map { s =>
                    val arr = s.split(',')
                    val pickup = new Point(arr(11).toDouble,
                                           arr(10).toDouble)
                    val trip_time = arr(8).toInt
                    val trip_distance = arr(9).toDouble
                    val cost = getCost(trip_time, trip_distance)
                    val b = getBorough(pickup)
                    (b, cost)
                  }.aggregateByKey((0d, 0), 4)(
                    // optimized groupByKey.map(_.avg)
                    {case ((sum, count), next) => (sum + next, count+1)},
                    {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                  ).mapValues({case (sum, count) => sum.toDouble/count})
    // jteoh: slight edit to print out timing information
    val output = Lineage.measureTimeWithCallback(waitTime.collect(),
                                                 x=>println(s"Collect time: $x ms"))
                  
    output.take(25).foreach(println)
  }
  
  //groupby license
  // sort each group by ascending pickup time
  //sliding window(2) == > wait time
  // get borough
  // group by borough
  // average wait time
  
  def getCost(time: Int, distance: Double): Double = {
    (time * 0.019f) + (distance * 5.5f)
  }
  
  def getBorough(p: Point): Int = {
    if (isInside(manhattan, 4, p)) {
      return 1
    } else if (isInside(brooklyn, 5, p)) {
      return 2
    } else return 3
  }
  
  // Returns true if the point p lies inside the polygon[] with n vertices
  def isInside(polygon: Array[Point], n: Int, p: Point): Boolean = { // There must be at least 3 vertices in polygon[]
    if (n < 3) return false
    // Create a point for line segment from p to infinite
    val extreme = Point(Double.MaxValue, p.y)
    // Count intersections of the above line with sides of polygon
    var count = 0
    var i = 0
    do {
      val next = (i + 1) % n
      // Check if the line segment from 'p' to 'extreme' intersects
      // with the line segment from 'polygon[i]' to 'polygon[next]'
      if (doIntersect(polygon(i), polygon(next), p, extreme)) { // If the point 'p' is colinear with line segment 'i-next',
        // then check if it lies on segment. If it lies, return true,
        // otherwise false
        if (orientation(polygon(i), p, polygon(next)) == 0)
          return onSegment(polygon(i), p, polygon(next))
        count += 1
      }
      i = next
    } while ({
      i != 0
    })
    // Return true if count is odd, false otherwise
    count % 2 == 1 // Same as (count%2 == 1)
    
  }
  
  // The function that returns true if line segment 'p1q1'
  // and 'p2q2' intersect.
  def doIntersect(p1: Point, q1: Point, p2: Point, q2: Point): Boolean = { // Find the four orientations needed for general and
    // special cases
    val o1 = orientation(p1, q1, p2)
    val o2 = orientation(p1, q1, q2)
    val o3 = orientation(p2, q2, p1)
    val o4 = orientation(p2, q2, q1)
    // General case
    if (o1 != o2 && o3 != o4) return true
    // Special Cases
    // p1, q1 and p2 are colinear and p2 lies on segment p1q1
    if (o1 == 0 && onSegment(p1, p2, q1)) return true
    // p1, q1 and p2 are colinear and q2 lies on segment p1q1
    if (o2 == 0 && onSegment(p1, q2, q1)) return true
    // p2, q2 and p1 are colinear and p1 lies on segment p2q2
    if (o3 == 0 && onSegment(p2, p1, q2)) return true
    // p2, q2 and q1 are colinear and q1 lies on segment p2q2
    if (o4 == 0 && onSegment(p2, q1, q2)) return true
    false // Doesn't fall in any of the above cases
    
  }
  
  // Given three colinear points p, q, r, the function checks if
  // point q lies on line segment 'pr'
  def onSegment(p: Point, q: Point, r: Point): Boolean = {
    return (q.x <= max(p.x, r.x) && q.x >= min(p.x, r.x)) &&
      (q.y <= max(p.y, r.y) && q.y >= min(p.y, r.y))
  }
  
  // To find orientation of ordered triplet (p, q, r).
  // The function returns following values
  // 0 --> p, q and r are colinear
  // 1 --> Clockwise
  // 2 --> Counterclockwise
  def orientation(p: Point, q: Point, r: Point): Double = {
    
    val v = (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y)
    if (v == 0) return 0 // colinear
    if (v > 0) 1
    else 2 // clock or counterclock wise
    
  }
  case class Point(x: Double, y: Double)
  
  /** private[spark] class KVAvg[K, V](self: Lineage[(K, V)])
   * (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
   * extends PairLRDDFunctions[K, V](self) */
  
//  override def cmdLineDelay(x: String): String = {
//    // override to check by hashcode, which should be good enough for uniqueness
//    // 6/2 1:30AM: I suspect this doesn't work because the delayTarget isn't being broadcast
//    // properly?? anyways, just hardcode for now.
//
////    if(delayTarget.map(_.toInt).contains(x.hashCode())) {
//    if(x.hashCode() == ) {
//      //Thread.sleep(delayTime.get)
//      Thread.sleep(HARDCODED_DELAY * 1000) // 60s formerly
//    }
//    x
//  }
}
