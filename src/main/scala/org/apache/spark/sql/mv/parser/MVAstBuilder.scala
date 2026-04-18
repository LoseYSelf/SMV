package org.apache.spark.sql.mv.parser

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.parser.MaterializedViewSqlParser._
import org.apache.spark.sql.catalyst.parser.MaterializedViewSqlBaseVisitor
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.mv.model._
import org.apache.spark.sql.mv.plan.logical._
import org.apache.spark.sql.SparkSession

class MVAstBuilder(sparkSession: SparkSession, queryText: String)
    extends MaterializedViewSqlBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitCreateMaterializedView(
      ctx: CreateMaterializedViewContext): LogicalPlan = {
    val identifiers = ctx.mvIdentifier().asScala
    val identifier = visitMvIdentifier(identifiers(0))
    val ifNotExists = ctx.IF() != null && ctx.NOT() != null
    val orReplace = ctx.OR() != null && ctx.REPLACE() != null
    val comment = Option(ctx.COMMENT()).map(_ => unquoteString(ctx.STRING().getText))
    val provider = if (ctx.USING() != null && identifiers.size > 1) {
      Some(visitMvIdentifier(identifiers(1)).unquotedString)
    } else None
    val options = if (ctx.OPTIONS() != null) {
      visitPropertyList(ctx.propertyList(0))
    } else Map.empty[String, String]
    val partitionColumns = if (ctx.PARTITIONED() != null) {
      visitParenIdentifierList(ctx.parenIdentifierList())
    } else Seq.empty[String]
    val tableProperties = if (ctx.TBLPROPERTIES() != null) {
      val propLists = ctx.propertyList().asScala
      if (ctx.OPTIONS() != null && propLists.size > 1) {
        visitPropertyList(propLists(1))
      } else if (ctx.OPTIONS() == null && propLists.nonEmpty) {
        visitPropertyList(propLists(0))
      } else Map.empty[String, String]
    } else Map.empty[String, String]

    CreateMaterializedViewCommand(
      identifier = identifier,
      queryText = queryText,
      ifNotExists = ifNotExists,
      orReplace = orReplace,
      comment = comment,
      provider = provider,
      options = options,
      partitionColumns = partitionColumns,
      tableProperties = tableProperties
    )
  }

  override def visitAlterMVRename(ctx: AlterMVRenameContext): LogicalPlan = {
    val identifiers = ctx.mvIdentifier().asScala
    val identifier = visitMvIdentifier(identifiers(0))
    val newName = visitMvIdentifier(identifiers(1))
    AlterMaterializedViewCommand(
      identifier = identifier,
      renameTo = Some(newName),
      setProperties = Map.empty,
      unsetProperties = Seq.empty,
      ifExists = false
    )
  }

  override def visitAlterMVSetProperties(ctx: AlterMVSetPropertiesContext): LogicalPlan = {
    val identifier = visitMvIdentifier(ctx.mvIdentifier())
    val properties = visitPropertyList(ctx.propertyList())
    AlterMaterializedViewCommand(
      identifier = identifier,
      renameTo = None,
      setProperties = properties,
      unsetProperties = Seq.empty,
      ifExists = false
    )
  }

  override def visitAlterMVUnsetProperties(
      ctx: AlterMVUnsetPropertiesContext): LogicalPlan = {
    val identifier = visitMvIdentifier(ctx.mvIdentifier())
    val keys = visitParenIdentifierList(ctx.parenIdentifierList())
    val ifExists = ctx.IF() != null && ctx.EXISTS() != null
    AlterMaterializedViewCommand(
      identifier = identifier,
      renameTo = None,
      setProperties = Map.empty,
      unsetProperties = keys,
      ifExists = ifExists
    )
  }

  override def visitDropMaterializedView(ctx: DropMaterializedViewContext): LogicalPlan = {
    val identifier = visitMvIdentifier(ctx.mvIdentifier())
    val ifExists = ctx.IF() != null && ctx.EXISTS() != null
    DropMaterializedViewCommand(identifier = identifier, ifExists = ifExists)
  }

  override def visitRefreshMaterializedView(
      ctx: RefreshMaterializedViewContext): LogicalPlan = {
    val identifier = visitMvIdentifier(ctx.mvIdentifier())
    RefreshMaterializedViewCommand(identifier = identifier)
  }

  override def visitShowMaterializedViews(
      ctx: ShowMaterializedViewsContext): LogicalPlan = {
    val namespace = Option(ctx.identifier()).map(_.getText)
    ShowMaterializedViewsCommand(namespace = namespace)
  }

  override def visitMvIdentifier(ctx: MvIdentifierContext): MVIdentifier = {
    val identifiers = ctx.identifier().asScala
    if (identifiers.size == 2) {
      MVIdentifier(Some(visitIdentifierNode(identifiers(0))), visitIdentifierNode(identifiers(1)))
    } else {
      MVIdentifier(None, visitIdentifierNode(identifiers(0)))
    }
  }

  private def visitIdentifierNode(ctx: IdentifierContext): String = {
    if (ctx.IDENTIFIER() != null) {
      ctx.IDENTIFIER().getText
    } else if (ctx.BACKQUOTED_IDENTIFIER() != null) {
      unquoteBacktick(ctx.BACKQUOTED_IDENTIFIER().getText)
    } else {
      ctx.getText
    }
  }

  override def visitParenIdentifierList(ctx: ParenIdentifierListContext): Seq[String] = {
    ctx.identifier().asScala.map(visitIdentifierNode).toSeq
  }

  override def visitPropertyList(ctx: PropertyListContext): Map[String, String] = {
    ctx.property().asScala.map { propCtx =>
      val key = visitPropertyKey(propCtx.propertyKey())
      val value = visitPropertyValue(propCtx.propertyValue())
      key -> value
    }.toMap
  }

  override def visitPropertyKey(ctx: PropertyKeyContext): String = {
    if (ctx.identifier() != null) {
      visitIdentifierNode(ctx.identifier())
    } else if (ctx.STRING() != null) {
      unquoteString(ctx.STRING().getText)
    } else {
      ctx.getText
    }
  }

  override def visitPropertyValue(ctx: PropertyValueContext): String = {
    if (ctx.INTEGER_VALUE() != null) {
      ctx.INTEGER_VALUE().getText
    } else if (ctx.STRING() != null) {
      unquoteString(ctx.STRING().getText)
    } else if (ctx.identifier() != null) {
      visitIdentifierNode(ctx.identifier())
    } else {
      ctx.getText
    }
  }

  private def unquoteString(s: String): String = {
    if (s.length >= 2 && s.startsWith("'") && s.endsWith("'")) {
      s.substring(1, s.length - 1).replace("\\'", "'")
    } else s
  }

  private def unquoteBacktick(s: String): String = {
    if (s.length >= 2 && s.startsWith("`") && s.endsWith("`")) {
      s.substring(1, s.length - 1).replace("``", "`")
    } else s
  }
}