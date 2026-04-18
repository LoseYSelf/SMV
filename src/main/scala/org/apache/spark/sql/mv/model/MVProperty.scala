package org.apache.spark.sql.mv.model

/**
 * A key-value property for a materialized view.
 * Used for TBLPROPERTIES and OPTIONS clauses in DDL statements.
 */
case class MVProperty(key: String, value: String)