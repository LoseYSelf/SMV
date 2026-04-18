package org.apache.spark.sql.mv.plan

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.mv.rewrite.MVCatalogManager
import org.apache.spark.sql.mv.model.{MVDefinition, MVState}
import org.apache.spark.sql.mv.plan.logical._

/**
 * Spark Strategy that handles execution of materialized view DDL commands.
 *
 * Each command is converted to an executable command that operates on
 * the MV catalog and underlying storage tables.
 */
class MVStrategy(sparkSession: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case cmd: CreateMaterializedViewCommand =>
      ExecutedCommandExec(() => executeCreate(cmd)) :: Nil
    case cmd: AlterMaterializedViewCommand =>
      ExecutedCommandExec(() => executeAlter(cmd)) :: Nil
    case cmd: DropMaterializedViewCommand =>
      ExecutedCommandExec(() => executeDrop(cmd)) :: Nil
    case cmd: RefreshMaterializedViewCommand =>
      ExecutedCommandExec(() => executeRefresh(cmd)) :: Nil
    case cmd: ShowMaterializedViewsCommand =>
      ExecutedCommandExec(() => executeShow(cmd)) :: Nil
    case _ => Nil
  }

  private def executeCreate(cmd: CreateMaterializedViewCommand): Unit = {
    val catalog = MVCatalogManager.getCatalog(sparkSession)

    if (cmd.ifNotExists && catalog.exists(cmd.identifier)) {
      return
    }

    if (!cmd.ifNotExists && catalog.exists(cmd.identifier)) {
      if (cmd.orReplace) {
        catalog.remove(cmd.identifier)
      } else {
        throw new IllegalArgumentException(
          s"Materialized view '${cmd.identifier.quotedString}' already exists")
      }
    }

    // Parse and analyze the query text using Spark's native parser
    val queryPlan = sparkSession.sessionState.sqlParser.parsePlan(cmd.queryText)
    val analyzedPlan = sparkSession.sessionState.analyzer.execute(queryPlan)

    // Create the underlying storage table
    val tableName = s"__mv_${cmd.identifier.database.getOrElse("default")}_${cmd.identifier.name}"
    val provider = cmd.provider.getOrElse("parquet")
    val createTableDDL = buildCreateTableDDL(tableName, analyzedPlan, provider, cmd)
    sparkSession.sql(createTableDDL)

    // Load initial data into the storage table
    sparkSession.sql(s"INSERT INTO TABLE $tableName ${cmd.queryText}")

    // Register the MV definition in the catalog
    val definition = MVDefinition(
      identifier = cmd.identifier,
      queryPlan = analyzedPlan,
      outputSchema = analyzedPlan.output,
      tableName = tableName,
      comment = cmd.comment,
      provider = Some(provider),
      options = cmd.options,
      partitionColumns = cmd.partitionColumns,
      tableProperties = cmd.tableProperties
    )
    catalog.create(definition, cmd.ifNotExists)
  }

  private def executeAlter(cmd: AlterMaterializedViewCommand): Unit = {
    val catalog = MVCatalogManager.getCatalog(sparkSession)

    cmd.renameTo.foreach { newName =>
      catalog.rename(cmd.identifier, newName)
    }

    if (cmd.setProperties.nonEmpty) {
      catalog.update(cmd.identifier) { defn =>
        defn.copy(tableProperties = defn.tableProperties ++ cmd.setProperties)
      }
    }

    if (cmd.unsetProperties.nonEmpty) {
      catalog.update(cmd.identifier) { defn =>
        defn.copy(tableProperties = defn.tableProperties -- cmd.unsetProperties)
      }
    }
  }

  private def executeDrop(cmd: DropMaterializedViewCommand): Unit = {
    val catalog = MVCatalogManager.getCatalog(sparkSession)

    if (cmd.ifExists && !catalog.exists(cmd.identifier)) {
      return
    }

    val definition = catalog.getOrThrow(cmd.identifier)

    // Drop the underlying storage table
    sparkSession.sql(s"DROP TABLE IF EXISTS ${definition.tableName}")

    // Remove from catalog
    catalog.remove(cmd.identifier)
  }

  private def executeRefresh(cmd: RefreshMaterializedViewCommand): Unit = {
    val catalog = MVCatalogManager.getCatalog(sparkSession)

    val definition = catalog.getOrThrow(cmd.identifier)

    // Update state to REFRESHING
    catalog.update(cmd.identifier)(_.withState(MVState.REFRESHING))

    try {
      // Truncate and reload the underlying table
      sparkSession.sql(s"TRUNCATE TABLE ${definition.tableName}")
      sparkSession.sql(s"INSERT INTO TABLE ${definition.tableName} " +
        definition.queryPlan.toString)

      // Mark as FRESH
      catalog.update(cmd.identifier)(_.withRefreshed())
    } catch {
      case e: Exception =>
        catalog.update(cmd.identifier)(_.withState(MVState.STALE))
        throw e
    }
  }

  private def executeShow(cmd: ShowMaterializedViewsCommand): Unit = {
    val catalog = MVCatalogManager.getCatalog(sparkSession)
  }

  private def buildCreateTableDDL(
      tableName: String,
      queryPlan: LogicalPlan,
      provider: String,
      cmd: CreateMaterializedViewCommand): String = {
    val schema = queryPlan.schema
    val columns = schema.fields.map { f =>
      s"${f.name} ${f.dataType.simpleString}"
    }.mkString(", ")

    val partitionClause = if (cmd.partitionColumns.nonEmpty) {
      s"PARTITIONED BY (${cmd.partitionColumns.mkString(", ")})"
    } else ""

    val propertiesClause = if (cmd.tableProperties.nonEmpty) {
      val props = cmd.tableProperties.map { case (k, v) => s"'$k'='$v'" }.mkString(", ")
      s"TBLPROPERTIES ($props)"
    } else ""

    val commentClause = cmd.comment.map(c => s"COMMENT '$c'").getOrElse("")

    s"CREATE TABLE $tableName ($columns) USING $provider " +
      partitionClause + " " + commentClause + " " + propertiesClause
  }
}

/**
 * A simple SparkPlan that executes a side-effecting function.
 */
case class ExecutedCommandExec(fn: () => Unit) extends SparkPlan {
  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil
  override def children: Seq[SparkPlan] = Nil
  override protected def doExecute(): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    fn()
    sparkContext.emptyRDD
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = this
}
