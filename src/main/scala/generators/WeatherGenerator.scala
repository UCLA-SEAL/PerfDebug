package generators

import org.apache.spark.SparkContext

class WeatherGenerator {
  val sc = new SparkContext()
  
  /** Copy everything below this */
  // Sample row of original data: 18090,30/12/2016,3984 mm
  // Intended new row sample: 18090,30/12/2016,3984 mm,cloudy
  import scala.util.Random
  
  
  /** Dataset values **/
  val ALPHABET_LIST = ('a' to 'z').toArray
  val MONTH_LENGTHS= Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31).map(1 to _)
  val MONTHS = (1 to MONTH_LENGTHS.length)
  val YEARS = (1915 to 2015)
  val SNOWFALL_MM_RANGE = (50f, 4000f)
  val FT_TO_MM_CONVERSION = 304.8f
  val SNOWFALL_FT_RANGE =
    (SNOWFALL_MM_RANGE._1 / FT_TO_MM_CONVERSION, SNOWFALL_MM_RANGE._2 / FT_TO_MM_CONVERSION)
  val ZIPCODES_ALL = (501 to 99950)
  val WEATHER_CONDITIONS = Array("sunny", "cloudy", "windy", "rainy", "snowy")
  
  val BIAS_STATE = 28
  val BIAS_MONTH = 4
  val BIAS_DAY = 15
  val ZIPCODES_BIASED = ZIPCODES_ALL.filter(_ % 50 == BIAS_STATE)
  
  
  
  /** simple method for random choice. Not designed to be efficient - I hope size is precomputed! */
  def choice[T](coll: Seq[T]): T = {
    coll(Random.nextInt(coll.size))
  }
  
  def randFloatInRange(range: Tuple2[Float, Float]): Float = {
    val (start, end) = range
    start + (end - start) * Random.nextFloat()
  }
  
  def randomDate(days: Seq[Seq[Int]], months: Seq[Int], years: Seq[Int]): String = {
    // unused
    val month = choice(months)
    val day = choice(days(month-1))
    val year = choice(years)
    s"$day/$month/$year"
  }
  
  def randomSnowfall(): String = {
    if(Random.nextBoolean()) {
      // mm
      s"${randFloatInRange(SNOWFALL_MM_RANGE)} mm"
    } else {
      // ft
      s"${randFloatInRange(SNOWFALL_FT_RANGE)} ft"
    }
  }
  
  def randomAlphaString(size: Int): String = {
    (1 to size).map(_ => choice(ALPHABET_LIST)).mkString
  }
  
  /** Configurations **/
  /* Output file */
  
  
  
  
  /* Data generation */
  def generateData(mPartitions: Int, mPartitionSize: Int,
                   targetFile: String,
                   biasRate: Double): Unit = {
    assert(0.0 <= biasRate && biasRate <= 1.0)
    println(s"Creating $mPartitions partitions, $mPartitionSize records per partition")
    sc.parallelize(Seq[Int](), mPartitions).mapPartitions { _ =>
      (1 to mPartitionSize).iterator.map({ _ =>
        // Intended new row sample: 18090,30/12/2016,3984 mm,cloudy
        // biased fields are zipcode + monthdate
        val (zipCode, month, day) = if(Random.nextDouble() <= biasRate) {
          // bias case
          (choice(ZIPCODES_BIASED), BIAS_MONTH, BIAS_DAY)
        } else {
          // normal case
          var thisMonth = choice(MONTHS)
          (choice(ZIPCODES_ALL), thisMonth, choice(MONTH_LENGTHS(thisMonth-1)))
        }
        
        val year = choice(YEARS)
        
        val snowfall = randomSnowfall()
        val condition = choice(WEATHER_CONDITIONS)
        Seq(zipCode, s"$month/$day/$year", snowfall, condition).mkString(",")
      })
    }.saveAsTextFile(targetFile)
  }
  
  /* Dataset size */
  // initial test parameters
  // val partitions = 5 // Testing
  // val partitionSize  = 10 * 1000 * 1000 // Testing
  // Show work here
  // 5 * 10M = 1.6GB. Let's test it out with a 50GB setup?
  // ie about 32MB per 1M rows
  // let's try 200 partitions, in which case we want roughly 250MB per partition
  // at 32MB per 1M rows, that's roughly 8x or 8M rows?
  //  val partitions = 200
  //  val partitionSize = 8 * 1000 * 1000
  
  // if we wanted less data, say a 15 GB setup:
  val partitions = 200
  val partitionSize = 2350 * 1000 // 2.35M
  // alternately: just take 60 partitions from the previous setup
  
  
  val biasFraction = 0.10 // 0 to 1
  
  val outputTarget = s"hdfs://buckeye.cs.ucla.edu:9002/user/plsys-jia/weatherGenSmall_${biasFraction}"
  generateData(partitions, partitionSize, outputTarget, biasFraction)
}
