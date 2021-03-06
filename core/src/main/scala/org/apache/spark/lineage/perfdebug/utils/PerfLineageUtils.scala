package org.apache.spark.lineage.perfdebug.utils

import org.apache.spark.lineage.perfdebug.lineageV2.LineageWrapper._
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD

object PerfLineageUtils {
  
  /** Join backwards in data lineage until the original data source (or first/leftmost), then
   * join to the raw hadoop data. Print and return RDD.
   *
   * Key parts that are still required and not externalized:
   * Execution graph (how the caches relate to each other)
   * raw data RDDs
   */
  def traceBackAndPrint(rdd: Lineage[_], rawInputRdd: RDD[(Long, String)]): RDD[(Long, String)]
  = {
    val lastLineageWrapper = rdd.lineageWrapper
    println("----------Tap dependencies---------")
    lastLineageWrapper.printDependencies() // debugging
    
    // trace to the data source (hadoop)
    val hadoopLineageWrapper = lastLineageWrapper.traceBackAll()
    val rawHadoopValues = hadoopLineageWrapper.joinInputRDD(rawInputRdd)
    printRDDWithMessage(rawHadoopValues, "Raw input data that was traced (with byte offsets):")
    rawHadoopValues
  }
  
  
  
  /* Utility method to print a list of cache values with their schema. By default, will print
  out the count at the end.
   */
  def printCacheValues[V <: CacheValue](values: Traversable[V], withCount: Boolean = true): Unit = {
    if (values.nonEmpty) {
      val schema = values.head match {
        case _: TapLRDDValue => TapLRDDValue.readableSchema
        case _: TapHadoopLRDDValue => TapHadoopLRDDValue.readableSchema
        case _: TapPreShuffleLRDDValue => TapPreShuffleLRDDValue.readableSchema
        case _: TapPostShuffleLRDDValue => TapPostShuffleLRDDValue.readableSchema
        case _ => "WARNING: Undefined schema"
      }
      println(schema)
    }
    values.foreach(v => println("\t" + v))
    println("Printed count: " + values.size)
  }
  
  def printRDDWithMessage(rdd: RDD[_], msg: String, printSeparatorLines: Boolean = true, limit:
  Option[Int] = None, cacheRDD: Boolean = false): Unit = {
    // TODO WARNING - ENABLING CACHE FLAG REALLY SCREWS THINGS UP FOR UNKNOWN REASONS
    
    // The general assumption is the input RDD will also be used later, so cache it pre-emptively
    if(cacheRDD) rdd.cache()
    // collect output before printing because of console output
    val values = if(limit.isDefined) {
      //rdd.take(limit.get) // not supported in lineage :(
      
      rdd.collect().take(limit.get) // yikes
    } else {
      rdd.collect()
    }
    val numResults = values.length
    val sepChar = "-"
    val sepStr = sepChar * 100
    
    val resultCountStr =
      limit.filter(_ == numResults).map("Up to " + _).getOrElse("All " + numResults) + " result(s) shown"
    if(printSeparatorLines) println(sepStr)
    
    println(msg)
    values.foreach(s => println("\t" + s))
    println(resultCountStr)
    if(printSeparatorLines) println(sepStr)
    
  }
  
  /** Join two key-ed RDDs assuming their partitions line up. By default, the left RDD is assumed
   * to be smaller and used to build an in-memory map of values for joining.
   */
  def joinByPartitions[K,V1,V2](left: RDD[(K, V1)],
                                right: RDD[(K, V2)],
                                leftSmaller: Boolean = true): RDD[(K, (V1, V2))] = {
    if(!leftSmaller) {
      joinByPartitions(right, left, leftSmaller = false).map({case (k, v) => (k, v.swap)})
    }
    else {
      right.zipPartitions(left) { (buildIter, streamIter) =>
        val slowestRecordsMap = buildIter.toMap
        if (slowestRecordsMap.isEmpty) {
          Iterator.empty
        } else {
          streamIter.flatMap({ case (offset, text) =>
            slowestRecordsMap.get(offset).map(score => (offset, (text, score)))
            // fun scala: this is either an empty or singleton because get
            // returns an option!
          })
        }
      }
    }
  }
  
  def joinByPartitionsOld[K,V1,V2](prev: RDD[(K, V1)], baseRDD: RDD[(K, V2)]): RDD[(K, (V1, V2))]  = {
    baseRDD.zipPartitions(prev) {
      (buildIter, streamIter) =>
        val hashMap = new java.util.HashMap[K,V2]()
        var rowKey: (K, V2) = null.asInstanceOf[(K, V2)]
        
        // Create a Hash map of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()
          val keyExists = hashMap.containsKey(rowKey._1)
          if (!keyExists) {
            hashMap.put(rowKey._1, rowKey._2)
          }
        }
        
        if (hashMap.isEmpty) {
          Iterator.empty
        } else {
          streamIter.flatMap(current => {
            if(!hashMap.containsKey(current._1)) {
              Seq.empty
            } else {
              // assuming buildIter should be the 2nd value
              val v2 = hashMap.get(current._1)
              Seq((current._1, (current._2, v2)))
            }
          })
          
        }
    }
  }
}
