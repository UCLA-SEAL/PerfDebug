package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.lineage.perfdebug.ignite.IgniteLineageCacheRepository
import org.apache.spark.SparkContext
import org.apache.spark.lineage.perfdebug.storage.PerfLineageCacheStorage
import org.apache.spark.lineage.rdd.TapLRDD

/**
 * Unlike AggregateStatsStorage or PerfLineageCacheStorage, this class is only used from the
 * driver (as opposed to executors/tasks) and is thus more easily configured.
 */
trait LineageCacheRepository {
  
  def getCache(name: String): LineageCache
  
  def getCacheDependencies(jobId: String): LineageCacheDependencies
  def saveCacheDependencies(jobId: String, lineageCacheDependencies: LineageCacheDependencies): Unit
  
  def close(): Unit
}

object LineageCacheRepository {
  private[this] var _cacheRepository: Option[LineageCacheRepository] = None
  
  def cacheRepository: Option[LineageCacheRepository] = _cacheRepository
  
  def setCacheRepository(value: LineageCacheRepository): Unit = {
    _cacheRepository = Some(value)
  }
  
  def useSimpleIgniteCacheRepository(sc: SparkContext): Unit = {
    setCacheRepository(
      new IgniteLineageCacheRepository(
        new IgniteContext(sc,() => new IgniteConfiguration())))
  }
  
  def getCache(tap: TapLRDD[_]): LineageCache = {
    // this will be lost after the session, and thus this method is discouraged
    val appId = tap.lineageContext.sparkContext.applicationId
    val name = PerfLineageCacheStorage.getInstance().buildCacheName(appId, tap)
    getCache(name)
  }
  
  def getCache(name: String): LineageCache =
    withCacheCheck { _.getCache(name) }
  
  
  def getCacheDependencies(appId: String): LineageCacheDependencies =
    withCacheCheck { _.getCacheDependencies(appId) }
  
  
  def saveCacheDependencies(appId: String, deps: LineageCacheDependencies): Unit =
    withCacheCheck { _.saveCacheDependencies(appId, deps) }
  
  def close(): Unit = {
    if(cacheRepository.isDefined)
      cacheRepository.get.close()
  }
  
  private def withCacheCheck[U](body: LineageCacheRepository => U): U = {
    if(cacheRepository.isDefined)
    {
      body(cacheRepository.get)
    }
    else {
      throw new IllegalStateException("External lineage cache repository must be defined via " +
                                        "setCacheRepository before lineage information can be " +
                                        "retrieved.")
    }
  }
  
}


