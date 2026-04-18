package org.apache.spark.sql.mv.catalog

import org.apache.spark.sql.mv.model.{MVDefinition, MVIdentifier, MVState}

/**
 * Catalog for materialized view metadata.
 * Manages the lifecycle of MV definitions including creation, lookup, update, and deletion.
 *
 * Uses MVInMemoryCatalogStore by default. Can be extended with
 * MVExternalCatalog for persistent storage.
 */
class MVCatalog(store: MVCatalogStore = new MVInMemoryCatalogStore) {

  /**
   * Register a new materialized view definition.
   * @throws IllegalArgumentException if the MV already exists
   */
  def create(definition: MVDefinition): Unit = {
    val key = definition.identifier.unquotedString
    if (store.get(key).isDefined) {
      throw new IllegalArgumentException(
        s"Materialized view '${definition.identifier.quotedString}' already exists")
    }
    store.put(key, definition)
  }

  /**
   * Register a new MV, silently ignoring if it already exists and ifNotExists is true.
   */
  def create(definition: MVDefinition, ifNotExists: Boolean): Unit = {
    if (ifNotExists) {
      val key = definition.identifier.unquotedString
      if (store.get(key).isEmpty) {
        store.put(key, definition)
      }
    } else {
      create(definition)
    }
  }

  /**
   * Look up an MV definition by identifier.
   */
  def get(identifier: MVIdentifier): Option[MVDefinition] = {
    store.get(identifier.unquotedString)
  }

  /**
   * Look up an MV definition by identifier, throwing if not found.
   */
  def getOrThrow(identifier: MVIdentifier): MVDefinition = {
    get(identifier).getOrElse(
      throw new IllegalArgumentException(
        s"Materialized view '${identifier.quotedString}' not found")
    )
  }

  /**
   * Remove an MV definition from the catalog.
   * @return true if the MV existed and was removed, false otherwise
   */
  def remove(identifier: MVIdentifier): Boolean = {
    store.remove(identifier.unquotedString)
  }

  /**
   * Remove an MV definition, optionally suppressing the not-found error.
   */
  def remove(identifier: MVIdentifier, ifExists: Boolean): Boolean = {
    if (ifExists) {
      store.remove(identifier.unquotedString)
      true
    } else {
      val existed = store.remove(identifier.unquotedString)
      if (!existed) {
        throw new IllegalArgumentException(
          s"Materialized view '${identifier.quotedString}' not found")
      }
      true
    }
  }

  /**
   * Update an existing MV definition (e.g., rename, change properties, change state).
   */
  def update(identifier: MVIdentifier)(f: MVDefinition => MVDefinition): MVDefinition = {
    val key = identifier.unquotedString
    store.get(key) match {
      case Some(existing) =>
        val updated = f(existing)
        store.put(key, updated)
        updated
      case None =>
        throw new IllegalArgumentException(
          s"Materialized view '${identifier.quotedString}' not found")
    }
  }

  /**
   * Rename an MV: remove old entry, insert with new identifier.
   */
  def rename(oldIdentifier: MVIdentifier, newIdentifier: MVIdentifier): Unit = {
    val oldKey = oldIdentifier.unquotedString
    val newKey = newIdentifier.unquotedString
    store.get(oldKey) match {
      case Some(existing) =>
        store.remove(oldKey)
        store.put(newKey, existing.copy(identifier = newIdentifier))
      case None =>
        throw new IllegalArgumentException(
          s"Materialized view '${oldIdentifier.quotedString}' not found")
    }
  }

  /**
   * List all MV definitions in the catalog.
   */
  def listAll(): Seq[MVDefinition] = {
    store.list()
  }

  /**
   * List MV definitions filtered by namespace (database).
   */
  def listByNamespace(namespace: String): Seq[MVDefinition] = {
    store.list().filter(_.identifier.database.contains(namespace))
  }

  /**
   * List only MV definitions eligible for query rewriting (FRESH state).
   */
  def listRewritable: Seq[MVDefinition] = {
    store.list().filter(_.isRewritable)
  }

  /**
   * Check if an MV exists.
   */
  def exists(identifier: MVIdentifier): Boolean = {
    store.get(identifier.unquotedString).isDefined
  }
}

/**
 * Trait for MV catalog storage backends.
 */
trait MVCatalogStore {
  def get(key: String): Option[MVDefinition]
  def put(key: String, definition: MVDefinition): Unit
  def remove(key: String): Boolean
  def list(): Seq[MVDefinition]
}