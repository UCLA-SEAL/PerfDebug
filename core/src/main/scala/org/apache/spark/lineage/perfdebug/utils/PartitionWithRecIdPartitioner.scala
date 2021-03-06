package org.apache.spark.lineage.perfdebug.utils

import org.apache.spark.Partitioner
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.PartitionWithRecId

/** Extremely simple partitioner that assumes the key is the exact partition in question. */
class PartitionWithRecIdPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions
  
  override def getPartition(key: Any): Int = key.asInstanceOf[PartitionWithRecId].partition
  
  override def equals(other: Any): Boolean = other match {
    case p: PRIdTuplePartitioner =>
      p.numPartitions == numPartitions
    case p: PartitionWithRecIdPartitioner =>
      p.numPartitions == numPartitions
    case _ =>
      false
  }
}
