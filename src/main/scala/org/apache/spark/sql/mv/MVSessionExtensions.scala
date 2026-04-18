package org.apache.spark.sql.mv

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.mv.parser.MVSqlParser
import org.apache.spark.sql.mv.plan.MVStrategy
import org.apache.spark.sql.mv.rewrite.MaterializedViewRewriteRule

/**
 * Spark Session Extension that injects materialized view support.
 *
 * Activates via:
 *   spark.sql.extensions = org.apache.spark.sql.mv.MVSessionExtensions
 * Or programmatically:
 *   SparkSession.builder().withExtensions(new MVSessionExtensions()).getOrCreate()
 *
 * Injects:
 * 1. Custom parser to handle MV DDL statements
 * 2. Optimizer rule to rewrite queries using materialized views
 * 3. Planning strategy to execute MV DDL commands
 */
class MVSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject custom parser for MV DDL
    extensions.injectParser { (session, delegate) =>
      new MVSqlParser(session, delegate)
    }

    // Inject optimizer rule for MV query rewriting
    extensions.injectOptimizerRule { session =>
      new MaterializedViewRewriteRule(session)
    }

    // Inject planning strategy for MV command execution
    extensions.injectPlannerStrategy { session =>
      new MVStrategy(session)
    }
  }
}