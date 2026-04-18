package org.apache.spark.sql.mv.catalog

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.mv.model.MVDefinition

/**
 * In-memory implementation of MVCatalogStore using ConcurrentHashMap.
 * This is the default storage backend, suitable for development and testing.
 * Data is lost when the Spark session ends.
 */
class MVInMemoryCatalogStore extends MVCatalogStore {

  private val store = new ConcurrentHashMap[String, MVDefinition]()

  override def get(key: String): Option[MVDefinition] = {
    Option(store.get(key))
  }

  override def put(key: String, definition: MVDefinition): Unit = {
    store.put(key, definition)
  }

  override def remove(key: String): Boolean = {
    store.remove(key) != null
  }

  override def list(): Seq[MVDefinition] = {
    store.values().asScala.toSeq
  }
}