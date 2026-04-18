package org.apache.spark.sql.mv.catalog

import org.apache.spark.sql.mv.model.MVDefinition

/**
 * External catalog interface for persistent MV metadata storage.
 * Implement this trait to integrate with external metadata stores
 * (e.g., Hive Metastore, JDBC database, etc.).
 *
 * Usage:
 *   val externalStore = new MyJdbcCatalogStore(...)
 *   val catalog = new MVCatalog(externalStore)
 */
trait MVExternalCatalog extends MVCatalogStore {

  /**
   * Initialize the external catalog (e.g., create tables if not exists).
   */
  def initialize(): Unit

  /**
   * Close resources used by the external catalog.
   */
  def close(): Unit
}