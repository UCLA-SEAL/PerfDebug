package generators

import org.apache.spark.SparkContext

/** Based off of fileGenerator.py and a scala-based parallelization script provided by Gulzar
 * (for another student project).
 * This is mostly meant to be run in spark shell as a one-off script.
 * https://github.com/maligulzar/bigdebug/blob/titian-bigsift/examples/src/main/scala/org/apache/spark/examples/bigsift/benchmarks/studentdataanalysis/datageneration/fileGenerator.py
 */
class StudentInfoGenerator {
  val sc = new SparkContext()
  
  /** Copy everything below this */
  import scala.util.Random
  
  
  /** Dataset values **/
  val ALPHABET_LIST = ('a' to 'z').toArray
  val FIRST_NAME_MAX_LENGTH = 10
  val LAST_NAME_MAX_LENGTH = 15
  val GENDER_LIST = Array("male", "female")
  val GRADE_LIST = ('0' to '3').toArray
  val GRADE_TO_AGE_MAP = GRADE_LIST.map(grade => {
    // age ranges are preserved from the original script (though simplified conditionwise)
    val lower = 18 + 2 * grade.toInt // 18, 20, 22, 24
    val upper = lower + 1
    grade -> Array(lower, upper)
  }).toMap
  
  val MAJOR_LIST = Array("English", "Mathematics", "ComputerScience", "ElectricalEngineering",
                       "Business", "Economics", "Biology", "Law", "PoliticalScience",
                       "IndustrialEngineering")
  
  /** simple method for random choice. Not designed to be efficient - I hope size is precomputed! */
  def choice[T](coll: Seq[T]): T = {
    coll(Random.nextInt(coll.size))
  }
  
  def randomAlphaString(size: Int): String = {
    (1 to size).map(_ => choice(ALPHABET_LIST)).mkString
  }
  
  /** Configurations **/
  /* Dataset size */
  val partitions = 30 // orig script: 400
  val partitionSize  = 20000000// orig script: 100000 ie 100K
  // estimate: 96.1MB for 50 partitions x 50K records/part (2.5M records), so 38.44B/record
  // Targeting: 20GB -> 520,291,363 records, so we want to increase dataset by roughly 200x
  // test: 200 partitions, 2.5M records per partition
  
  /* Output file */
  val outputTarget = "hdfs://zion-12.cs.ucla.edu:9002/user/plsys-jia/studentGrades"
  
  /* Dataset bias - if you want to skew the data towards one grade in particular */
  val biasFraction = 0.0 // 0 to 1
  assert(0.0 <= biasFraction && biasFraction <= 1.0)
  assert(biasFraction == 0.0, "Bias is not supported right now")
  val biasTarget = GRADE_LIST.head
  
  /* Data generation */
  def generateData(mPartitions: Int, mPartitionSize: Int): Unit = {
    println(s"Creating $mPartitions partitions, $mPartitionSize records per partition")
    sc.parallelize(Seq[Int](), mPartitions).mapPartitions { _ =>
      (1 to mPartitionSize).iterator.map({ _ =>
        
        val firstNameLength = Random.nextInt(FIRST_NAME_MAX_LENGTH) + 1
        val lastNameLength = Random.nextInt(LAST_NAME_MAX_LENGTH) + 1
        val firstName = randomAlphaString(firstNameLength)
        val lastName = randomAlphaString(lastNameLength)
        
        val gender = choice(GENDER_LIST)
        val major = choice(MAJOR_LIST)
        
        val grade = choice(GRADE_LIST)
        val age = choice(GRADE_TO_AGE_MAP(grade))
        
        s"$firstName $lastName $gender $age $grade $major"
        
        /*
          // old student data code, outdated.
          val course= (Random.nextInt(570) + 30).toString
          val f = if(Random.nextBoolean()) "EE" else "CS"
          val students = Random.nextInt(190) + 10
          var list_std = List[Int]()
          for(i <- 0 to students){
            list_std  =  (Random.nextInt(90)+10) :: list_std
          }
          val str = list_std.map(s => f+course +":"+s).reduce(_+","+_)
          str
        */
        
      })
    }.saveAsTextFile(outputTarget)
  }
  
  generateData(partitions, partitionSize)
}
