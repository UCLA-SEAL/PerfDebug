abstract class BaselineApp {
  final def main(args: Array[String]): Unit = {
    measureTimeWithCallback({
      run(args)
    }, x => println(s"Total time: $x ms"))
  }
  
  def run(args: Array[String]): Unit
  
  /** Executes the provided block and returns a pair of (result, time taken) where time is in ms*/
  def measureTime[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name: https://docs.scala-lang.org/tour/by-name-parameters.html
    val t1 = System.currentTimeMillis()
    val timeTaken = t1 - t0
    (result, timeTaken)
  }
  
  def measureTimeWithCallback[R](block: => R, callback: Long => Unit): R = {
    val (result, time) = measureTime(block)
    callback(time)
    result
  }
}
