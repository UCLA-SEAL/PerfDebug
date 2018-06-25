package org.apache.spark.lineage.ignite

import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import CacheDataTypes._



object IgniteCacheFactory {
  
  val ignite: Ignite = Ignition.ignite()

  def createTapLRDDCache: CacheArguments => IgniteCache[PartitionWithRecId, TapLRDDValue] =
    createIgniteCache[PartitionWithRecId, TapLRDDValue]
  
  
  def createTapPreShuffleRDDCache: CacheArguments => IgniteCache[Long, (Array[Long], Array[Long])] =
    createIgniteCache[Long, (Array[Long], Array[Long])]
  
  
  def createIgniteCache[K,V](cacheArguments: CacheArguments): IgniteCache[K,V] = {
    val cacheConf = new CacheConfiguration[K, V](cacheArguments.cacheName)
      .setAffinity(
        new RendezvousAffinityFunction(false, cacheArguments.numPartitionsPerCache)
      )
    
    // Split statements for ease of debugging and clarity with getOrCreateCache
    val cache: IgniteCache[K, V] = ignite.getOrCreateCache(cacheConf)
    cache
  }
}

// TODO number of cache partitions is currently fixed because the default 1024
// cannot be overridden globally or changed after creation, but is too high for local
// development. Using IgniteRDDs will result in one RDD partition per cache
// partition, and simple operations will end up spawning 1024 tasks.
case class CacheArguments(cacheName: String, numPartitionsPerCache: Int = 2)
