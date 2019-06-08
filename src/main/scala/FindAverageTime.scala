import org.apache.spark.{SparkConf, SparkContext}
import Math.{max, min}

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.Lineage
import LineageContext._
import org.apache.spark.broadcast.Broadcast

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
  // val HARDCODED_DELAY = 10
  // val HARDCODED_HASH = -102905216
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
  
  //val xCutoff = -73.989166// Matches about 0.1% of the dataset (of 173M)
  //val xCutoff = -74.00 // Matches about 0.04% of the dataset (of 173M)
  val xCutoff = -74.017082 // Matches 1000 rows, out of 173M or so. Roughly 0.0006%?
  val xMin = -5000.0  // -3547.9207
  val yMin = -5000.0  //  -3084.2959
  val xMax = 5000.0   // 3310.3645
  val yMax = 5000.0   // 2945.9587
  val baseTargetBorough = Array(Point(xMin, yMax),
                            Point(xCutoff, yMax),
                            Point(xCutoff, yMin),
                            Point(xMin, yMin)
  )
  
  // note: duplication factor must be odd or isInside will always return false!
  val dupeFactor = 5001// 501
  // choose List specifically for poor indexing behavior
  val targetBorough = (1 to dupeFactor).flatMap(_ => baseTargetBorough).toList
  var targetBoroughBroadcast: Broadcast[Seq[Point]] = _
  val complementBorough = Array(Point(xCutoff, yMax),
                                Point(xMax, yMax),
                                Point(xMax, yMin),
                                Point(xCutoff, yMin)
  )

  var logFile: String = _
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    if(args.headOption.isEmpty)  defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Documents/datasets/trip_data_1.csv")
    setDelayOpts(args)
    // defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
    defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}" +
                             s"DUPE=${dupeFactor}")
    //s"-HARDCODED_DELAY=${HARDCODED_DELAY}")
  }
  
  /** *
   * The goal is to find the average revenue generated from trips originating from each borough near new york
   */
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    val lines = lc.textFile(logFile)
//    println("Broadcasting target borough")
//    targetBoroughBroadcast = lc.sparkContext.broadcast(targetBorough)
//    println("Broadcasted target borough!")
//    def getBorough(p: Point): Int = {
//      if (isInside(manhattan, p)) {
//        return 1
//      } else if (isInside(brooklyn, p)) {
//        return 2
//      } else if (isInside(complementBorough, p)) {
//        return 3
//      } else if (isInside(targetBoroughBroadcast.value, p)) {
//        return 4
//      } else
//          return -1
//    }
    // Depr: uses groupByKey which is very expensive
    val waitTimeDepr: Lineage[(Int, Double)] = lines
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
  
    val waitTime: Lineage[(Int, Double)] = lines
                  .map { s =>
                    val arr = s.split(',')
                    val pickup = new Point(arr(11).toDouble,
                                           arr(10).toDouble)
                    val trip_time = arr(8).toInt
                    val trip_distance = arr(9).toDouble
                    val cost = getCost(trip_time, trip_distance)
                    val b = getBorough(pickup)
                    (b, cost)
                  }.aggregateByKey((0d, 0), 3)(
                    // optimized groupByKey.map(_.avg)
                    {case ((sum, count), next) => (sum + next, count+1)},
                    {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                  ).mapValues({case (sum, count) => sum.toDouble/count})
    // jteoh: slight edit to print out timing information
    //    waitTime.saveAsTextFile("hdfs://buckeye:9002/user/plsys-jia/taxi_output")
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
    if (isInside(manhattan, p)) {
      return 1
    } else if (isInside(brooklyn, p)) {
      return 2
    } else if (isInside(complementBorough, p)) {
      return 3
    } else if (isInside(targetBorough, p)) {
      return 4
    } else
        return -1
  }
  
  def getBoroughOriginal(p: Point): Int = {
    if (isInside(manhattan, p)) {
      return 1
    } else if (isInside(brooklyn, p)) {
      return 2
    } else return 3
  }
  
  // Returns true if the point p lies inside the polygon[] with n vertices
  // jt: https://en.wikipedia.org/wiki/Point_in_polygon
  // jt: also, removed n as an argument.
  def isInside(polygon: Seq[Point], p: Point): Boolean = { // There must be at least 3 vertices in
    // polygon[]
    val n = polygon.length
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
