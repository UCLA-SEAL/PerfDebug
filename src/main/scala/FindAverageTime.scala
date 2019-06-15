import org.apache.spark.{SparkConf, SparkContext}
import Math.{max, min}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
/**
 * Created by malig on 4/15/19.
 * Adapted from malig by jteoh on 5/17/2019
 * Original code (adapted by malig) from AAS textbook example.
 */
object FindAverageTime extends BaselineApp {
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
  
  /** *
   * The goal is to find the average revenue generated from trips originating from each borough near new york
   */
  def run(args: Array[String]): Unit = {
    val sc = new SparkConf()
    var logFile = ""
    if (args.length == 0) {
      sc.setMaster("local[*]")
      logFile = "/Users/jteoh/Documents/datasets/trip_data_1.csv"
    } else {
      logFile = args.head
    }
    sc.setAppName("GeoTime")
    val ctx = new SparkContext(sc)
    val lines = ctx.textFile(logFile)
    
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
    val waitTimeDepr: RDD[(Int, Double)] = lines
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
  
    val waitTime: RDD[(Int, Double)] = lines
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
    val out = measureTimeWithCallback({
      waitTime.collect()
    }, x => println(s"Collect time: $x ms"))
  
    // debugging
    for (o <- out.take(25)) {
      println( o._1 + " - " + o._2)
    }
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
}
