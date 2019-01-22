package generators

import org.apache.spark.SparkContext

/** Based off of fileGenerator.py and a scala-based parallelization script provided by Gulzar
 * (for another student project).
 * This is mostly meant to be run in spark shell as a one-off script.
 */
class StudentInfoGenerator {
  val sc = new SparkContext()
  
  /** Copy everything below this */
  import scala.util.Random
  var partitions = 400
  var dataper  = 100000
  sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
    (1 to dataper).map { _ =>
      val course= (Random.nextInt(570) + 30).toString
      val f = if(Random.nextBoolean()) "EE" else "CS"
      val students = Random.nextInt(190) + 10
      var list_std = List[Int]()
      for(i <- 0 to students){
        list_std  =  (Random.nextInt(90)+10) :: list_std
      }
      val str = list_std.map(s => f+course +":"+s).reduce(_+","+_)
      str
    }.toIterator
  }.saveAsTextFile("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/studentGrades")
}
