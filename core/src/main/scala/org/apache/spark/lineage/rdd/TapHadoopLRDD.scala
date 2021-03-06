/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.lineage.rdd

import org.apache.hadoop.io.LongWritable
import org.apache.spark._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.util.{LongIntByteBuffer, LongIntLongByteBuffer}
import org.apache.spark.util.PackIntIntoLong

private[spark]
class TapHadoopLRDD[K, V](@transient lc: LineageContext, @transient deps: Seq[Dependency[_]])
  extends TapLRDD[(K, V)](lc, deps) {

  def this(@transient prev: HadoopLRDD[_, _]) =
    this(prev.lineageContext, List(new OneToOneDependency(prev)))

  @transient private var buffer: LongIntByteBuffer = _

  // Jason - convert to be equivalent to TapLRDD in Long 2nd value
  // 7/13/18 update: rather than cast to long, pack with the split id. In normal Titian this is
  // not necessary because the subsequent TapLRDD will be in the same partition (and thus
  // zipPartitions can be used to more efficiently join).
  // Note: The 'input key' here is misleadingly treated as a (K,V) because there's no explicit
  // TapHadoopLRDD cache yet.
  // Note: The values here are actually backwards w.r.t. the normal format of (output, input). It
  // will be explicitly handled in the TapHadoopLRDDValue class.
  override def materializeBuffer: Array[Any] = buffer.iterator.toArray.map(r => (r._1,
    PackIntIntoLong(splitId, r._2)))

  override def initializeBuffer = buffer = new LongIntByteBuffer(tContext.getFromBufferPool())

  override def releaseBuffer() = {
    buffer.clear()
    tContext.addToBufferPool(buffer.getData)
  }
  
  // println("WARNING: TAPHADOOPLRDD TAP IS DISABLED")
  override def tap(record: (K, V)) = {
    tContext.currentInputId = newRecordId()
    // TODO measure the time taken for hadoop rows, which were read from the previous RDD
    // because that computation is sealed in HadoopRDD, we would likely need to wrap the iterator
    // in the TapLRDD.compute function
    val timeTaken = 0
    tContext.updateRDDRecordTime(firstParent.id, timeTaken)
    buffer.put(record._1.asInstanceOf[LongWritable].get, nextRecord)
    record
  }
}
