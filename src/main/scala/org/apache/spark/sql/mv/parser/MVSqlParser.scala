package org.apache.spark.sql.mv.parser

import org.antlr.v4.runtime.{CharStreams, CommonToken, CommonTokenStream, RecognitionException, Recognizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.{MaterializedViewSqlLexer, MaterializedViewSqlParser, ParseException, ParserInterface, SparkRecognitionException}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.StructType

/**
 * Parser for materialized view DDL statements, using a delegation pattern.
 *
 * Strategy:
 * 1. Check if the SQL starts with a MV keyword prefix
 * 2. If yes: extract the query text (after AS), parse the DDL prefix with ANTLR4,
 *    then delegate query parsing to Spark's native parser
 * 3. If no: delegate entirely to Spark's native parser
 */
class MVSqlParser(sparkSession: SparkSession, delegate: ParserInterface)
    extends ParserInterface {

  private val mvPrefixes = Seq(
    "CREATE MATERIALIZED",
    "ALTER MATERIALIZED",
    "DROP MATERIALIZED",
    "REFRESH MATERIALIZED",
    "SHOW MATERIALIZED"
  )

  override def parsePlan(sqlText: String): LogicalPlan = {
    val normalized = sqlText.trim.toUpperCase
    if (isMVStatement(normalized)) {
      parseMVStatement(sqlText)
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): org.apache.spark.sql.types.DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)

  private def isMVStatement(normalizedSql: String): Boolean = {
    mvPrefixes.exists(prefix => normalizedSql.startsWith(prefix))
  }

  private def parseMVStatement(sqlText: String): LogicalPlan = {
    val queryText = if (sqlText.trim.toUpperCase.startsWith("CREATE")) {
      extractQueryText(sqlText)
    } else {
      ""
    }

    val charStream = CharStreams.fromString(sqlText)
    val lexer = new MaterializedViewSqlLexer(charStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(MVParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new MaterializedViewSqlParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(MVParseErrorListener)

    val visitor = new MVAstBuilder(sparkSession, queryText)
    visitor.visit(parser.singleStatement()).asInstanceOf[LogicalPlan]
  }

  private def extractQueryText(sqlText: String): String = {
    val upperSql = sqlText.toUpperCase
    val asIndex = findASKeyword(upperSql)
    if (asIndex >= 0) {
      sqlText.substring(asIndex + 3).trim
    } else {
      ""
    }
  }

  private def findASKeyword(upperSql: String): Int = {
    var depth = 0
    var lastAS = -1
    var i = 0
    while (i < upperSql.length - 3) {
      val c = upperSql.charAt(i)
      if (c == '(') depth += 1
      else if (c == ')') depth -= 1
      else if (depth == 0 && upperSql.substring(i, i + 4) == " AS ") {
        lastAS = i
      }
      i += 1
    }
    lastAS
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)
}

object MVParseErrorListener extends org.antlr.v4.runtime.BaseErrorListener {
  override def syntaxError(
                            recognizer: Recognizer[_, _],
                            offendingSymbol: scala.Any,
                            line: Int,
                            charPositionInLine: Int,
                            msg: String,
                            e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    e match {
      case sre: SparkRecognitionException if sre.errorClass.isDefined =>
        throw new ParseException(None, start, stop, sre.errorClass.get, sre.messageParameters)
      case _ =>
        throw new ParseException(None, msg, start, stop)
    }
  }
}