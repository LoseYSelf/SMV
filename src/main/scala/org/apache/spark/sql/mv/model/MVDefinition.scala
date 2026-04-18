package org.apache.spark.sql.mv.model

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Identifier for a materialized view, supporting both qualified (db.name)
 * and unqualified (name) forms.
 */
case class MVIdentifier(database: Option[String], name: String) {
  def unquotedString: String = database.map(db => s"$db.$name").getOrElse(name)
  def quotedString: String = database.map(db => s"`$db`.`$name`").getOrElse(s"`$name`")
}

/**
 * Lifecycle state of a materialized view.
 * Only FRESH views participate in query rewriting.
 */
sealed trait MVState

object MVState {
  /** Data is up-to-date, eligible for rewriting */
  case object FRESH extends MVState
  /** Data is stale (base tables have been modified), not eligible for rewriting */
  case object STALE extends MVState
  /** Refresh is in progress, not eligible for rewriting */
  case object REFRESHING extends MVState
  /** Rewriting is explicitly disabled */
  case object DISABLED extends MVState

  def fromString(s: String): MVState = s.toUpperCase match {
    case "FRESH" => FRESH
    case "STALE" => STALE
    case "REFRESHING" => REFRESHING
    case "DISABLED" => DISABLED
    case _ => throw new IllegalArgumentException(s"Unknown MV state: $s")
  }
}

/**
 * Complete definition of a materialized view.
 *
 * @param identifier     Unique identifier (database + name)
 * @param queryPlan      The original analyzed query plan that defines the MV
 * @param outputSchema   Output attributes of the MV
 * @param tableName      Name of the underlying storage table
 * @param comment        Optional description
 * @param provider       Storage format provider, defaults to "parquet"
 * @param options        Storage options
 * @param partitionColumns Partition column names
 * @param tableProperties Table properties
 * @param state          Current lifecycle state
 * @param createdAt      Timestamp when MV was created (epoch millis)
 * @param lastRefreshedAt Timestamp of last refresh (epoch millis)
 */
case class MVDefinition(
    identifier: MVIdentifier,
    queryPlan: LogicalPlan,
    outputSchema: Seq[Attribute],
    tableName: String,
    comment: Option[String] = None,
    provider: Option[String] = Some("parquet"),
    options: Map[String, String] = Map.empty,
    partitionColumns: Seq[String] = Seq.empty,
    tableProperties: Map[String, String] = Map.empty,
    state: MVState = MVState.FRESH,
    createdAt: Long = System.currentTimeMillis(),
    lastRefreshedAt: Long = System.currentTimeMillis()) {

  /** Whether this MV is eligible for query rewriting */
  def isRewritable: Boolean = state == MVState.FRESH

  /** Create a copy with updated state */
  def withState(newState: MVState): MVDefinition = copy(state = newState)

  /** Create a copy with updated refresh timestamp */
  def withRefreshed(): MVDefinition = copy(
    state = MVState.FRESH,
    lastRefreshedAt = System.currentTimeMillis()
  )
}
