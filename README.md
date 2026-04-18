# Spark Materialized View (SMV)

[English](README.md) | [中文](README_CN.md)


test
## Overview

**Spark Materialized View (SMV)** is a query rewriting plugin for Apache Spark 3.3.2 that introduces first-class materialized view support. It enables automatic query rewriting using pre-computed materialized views, dramatically accelerating analytical workloads without requiring manual query changes.

The rewriting algorithm is adapted from Apache Calcite's `MaterializedViewRule` and implemented on top of Spark's Catalyst optimizer, supporting SPJG (Select-Project-Join-GroupBy) query patterns with three match modalities and aggregate rollup.

## Features

- **DDL Support**: Full `CREATE / ALTER / DROP / REFRESH / SHOW MATERIALIZED VIEW` syntax with ANTLR4 grammar
- **Automatic Query Rewriting**: Optimizer rule transparently rewrites queries to use materialized views
- **Three Match Modalities**:
  - `COMPLETE` — Query and MV reference the same table set
  - `QUERY_PARTIAL` — MV has extra tables (cardinality-preserving join verification)
  - `VIEW_PARTIAL` — Query has extra tables (compensation join above MV)
- **Aggregate Rollup**: SUM, MIN, MAX direct reuse; COUNT → SUM0; AVG → SUM/COUNT decomposition
- **Compensation Predicates**: Equality compensation (equivalence class coverage) and residual predicate compensation
- **Spark Extension Injection**: Plugs in via `spark.sql.extensions` — no Spark source modification needed
- **In-Memory Catalog**: Default ConcurrentHashMap-based metadata storage with pluggable persistence interface
- **Parquet Storage**: Default columnar storage format for MV data

## Architecture

```
SparkSession
  |
  +-- MVSessionExtensions (injection entry point)
  |     +-- MVSqlParser           - Custom parser for MV DDL (delegates non-MV SQL to Spark)
  |     +-- MaterializedViewRewriteRule - Optimizer rule for query rewriting
  |     +-- MVStrategy            - Planning strategy for DDL command execution
  |
  +-- Parser Layer
  |     +-- MaterializedViewSql.g4  - ANTLR4 grammar (DDL only, query delegated to Spark)
  |     +-- MVAstBuilder            - Parse tree -> LogicalPlan visitor
  |     +-- MVSqlParser             - Delegation pattern parser
  |
  +-- Catalog Layer
  |     +-- MVCatalog               - Metadata management (CRUD + state)
  |     +-- MVInMemoryCatalogStore  - Default ConcurrentHashMap storage
  |     +-- MVExternalCatalog       - Pluggable persistence interface
  |
  +-- Model Layer
  |     +-- MVDefinition            - Core definition (plan, schema, state, storage)
  |     +-- MVIdentifier            - Qualified name (db.name)
  |     +-- MVState                 - Lifecycle (FRESH / STALE / REFRESHING / DISABLED)
  |
  +-- Rewrite Layer
  |     +-- MaterializedViewRewriteRule - Top-level optimizer rule
  |     +-- SPJG Rewriting Engine
  |           +-- SPJGRewriter           - Core matching algorithm
  |           +-- EquivalenceClasses     - Union-find based equivalence tracking
  |           +-- MatchModality          - COMPLETE / QUERY_PARTIAL / VIEW_PARTIAL
  |           +-- CompensationPredicates - Equality + residual compensation
  |           +-- AggregateRollup        - Aggregation function rollup logic
  |           +-- TableMappingGenerator  - Query-to-MV table/attribute mapping
  |           +-- PredicateSplitter      - Equality vs. residual predicate separation
  |           +-- ExpressionLineage      - Expression source tracing through plan
  |
  +-- Plan Layer
        +-- MVRelation              - Leaf scan node for rewritten plans
        +-- Logical Commands        - Create/Alter/Drop/Refresh/Show MV
```

## Quick Start

### 1. Build

```bash
mvn clean package -DskipTests
```

Requirements:
- Java 8+
- Maven 3.6+
- Scala 2.12.15 (handled by Maven)
- Spark 3.3.2 (provided scope)

### 2. Enable the Extension

Add the JAR to your Spark classpath and configure the extension:

```bash
# Via spark-submit
spark-submit \
  --jars spark-materialized-view-0.1.0.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.mv.MVSessionExtensions \
  your-app.jar

# Via spark-shell
spark-shell \
  --jars spark-materialized-view-0.1.0.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.mv.MVSessionExtensions
```

Or programmatically:

```scala
import org.apache.spark.sql.mv.MVSessionExtensions

val spark = SparkSession.builder()
  .appName("MV Demo")
  .withExtensions(new MVSessionExtensions())
  .getOrCreate()
```

### 3. Create a Materialized View

```sql
-- Simple MV
CREATE MATERIALIZED VIEW mv_sales_by_region
AS SELECT region, SUM(amount) AS total_amount
   FROM sales GROUP BY region;

-- With options
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_stats
USING parquet
OPTIONS ('compression'='snappy')
TBLPROPERTIES ('refresh.interval'='1h')
COMMENT 'Daily statistics aggregation'
AS SELECT date, category, COUNT(*) AS cnt, AVG(price) AS avg_price
   FROM orders GROUP BY date, category;
```

### 4. Query Rewriting (Automatic)

When a materialized view exists, the optimizer automatically rewrites compatible queries:

```sql
-- Original query (hits base tables)
SELECT region, SUM(amount) FROM sales GROUP BY region;

-- Automatically rewritten to use MV (much faster)
-- The optimizer replaces the scan with MVRelation pointing to mv_sales_by_region
```

With compensation predicates:

```sql
-- MV: SELECT region, SUM(amount) FROM sales GROUP BY region
-- Query with extra filter:
SELECT region, SUM(amount) FROM sales WHERE region = 'US' GROUP BY region;

-- Rewritten to: SELECT * FROM mv_sales_by_region WHERE region = 'US'
```

With aggregate rollup:

```sql
-- MV: SELECT date, category, COUNT(*), AVG(price) FROM orders GROUP BY date, category
-- Query with coarser grouping:
SELECT category, SUM(COUNT), SUM(AVG_PRICE * COUNT) / SUM(COUNT) FROM orders GROUP BY category;

-- Rewritten to: re-aggregate MV data with fewer grouping columns
```

### 5. Manage Materialized Views

```sql
-- Refresh data
REFRESH MATERIALIZED VIEW mv_sales_by_region;

-- Rename
ALTER MATERIALIZED VIEW mv_sales_by_region RENAME TO mv_regional_sales;

-- Set properties
ALTER MATERIALIZED VIEW mv_regional_sales SET TBLPROPERTIES ('retention'='30d');

-- Unset properties
ALTER MATERIALIZED VIEW mv_regional_sales UNSET TBLPROPERTIES ('retention');

-- Drop
DROP MATERIALIZED VIEW IF EXISTS mv_regional_sales;

-- List views
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS FROM my_database;
```

## DDL Syntax Reference

### CREATE MATERIALIZED VIEW

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] mv_identifier
  [COMMENT 'description']
  [USING provider]
  [OPTIONS (key=value, ...)]
  [PARTITIONED BY (col1, col2, ...)]
  [TBLPROPERTIES (key=value, ...)]
  AS query
```

### ALTER MATERIALIZED VIEW

```sql
ALTER MATERIALIZED VIEW mv_identifier RENAME TO new_mv_identifier
ALTER MATERIALIZED VIEW mv_identifier SET TBLPROPERTIES (key=value, ...)
ALTER MATERIALIZED VIEW mv_identifier UNSET TBLPROPERTIES [IF EXISTS] (key1, key2, ...)
```

### DROP MATERIALIZED VIEW

```sql
DROP MATERIALIZED VIEW [IF EXISTS] mv_identifier
```

### REFRESH MATERIALIZED VIEW

```sql
REFRESH MATERIALIZED VIEW mv_identifier
```

### SHOW MATERIALIZED VIEWS

```sql
SHOW MATERIALIZED VIEWS [FROM|IN namespace]
```

## Rewriting Algorithm

### SPJG Matching Flow

```
Query Plan
  |
  +- 1. Validate SPJG-only operators (Filter/Project/Join/Aggregate/Scan)
  |
  +- 2. Extract metadata (tables, predicates, equivalence classes)
  |
  +- 3. Match modality determination
  |     +- COMPLETE:       queryTables == viewTables
  |     +- QUERY_PARTIAL:  viewTables superset queryTables (extra MV joins must be cardinality-preserving)
  |     +- VIEW_PARTIAL:   queryTables superset viewTables (compensation joins needed)
  |     +- NO_MATCH:       incompatible table sets
  |
  +- 4. Generate table mapping (attribute-level bijection)
  |
  +- 5. Compute compensation predicates
  |     +- Equality compensation: query equivalence classes subset of mapped view equivalence classes
  |     +- Residual compensation: query residuals not implied by mapped view residuals
  |
  +- 6. Compute aggregate rollup (if applicable)
  |
  +- 7. Build rewritten plan: MVRelation + Filter + Aggregate + Project + Join
```

### Aggregate Rollup Rules

| Query Aggregation | MV Storage     | Rollup Method                      |
|-------------------|----------------|------------------------------------|
| `SUM(x)`         | `SUM(x)`       | Direct reuse: `SUM(mv_sum)`        |
| `MIN(x)`         | `MIN(x)`       | Direct reuse: `MIN(mv_min)`        |
| `MAX(x)`         | `MAX(x)`       | Direct reuse: `MAX(mv_max)`        |
| `COUNT(x)`       | `COUNT(x)`     | `COALESCE(SUM(mv_count), 0)`       |
| `AVG(x)`         | `SUM(x)+COUNT(x)` | `SUM(mv_sum) / SUM(mv_count)`  |

Query grouping columns must be a subset of MV grouping columns.

### MV Lifecycle States

| State       | Rewritable | Description                          |
|-------------|------------|--------------------------------------|
| `FRESH`     | Yes        | Data is up-to-date                   |
| `STALE`     | No         | Base tables modified, data outdated  |
| `REFRESHING`| No         | Refresh in progress                  |
| `DISABLED`  | No         | Rewriting explicitly disabled        |

## Project Structure

```
src/
+-- main/
|   +-- antlr4/org/apache/spark/sql/catalyst/parser/
|   |   +-- MaterializedViewSql.g4          # ANTLR4 DDL grammar
|   +-- scala/org/apache/spark/sql/mv/
|   |   +-- MVSessionExtensions.scala        # Extension injection entry
|   |   +-- parser/
|   |   |   +-- MVSqlParser.scala            # Delegation parser
|   |   |   +-- MVAstBuilder.scala           # Parse tree -> LogicalPlan
|   |   +-- catalog/
|   |   |   +-- MVCatalog.scala              # Metadata management
|   |   |   +-- MVInMemoryCatalogStore.scala # In-memory storage
|   |   |   +-- MVExternalCatalog.scala      # Persistence interface
|   |   +-- model/
|   |   |   +-- MVDefinition.scala           # Core model classes
|   |   |   +-- MVProperty.scala             # Property model
|   |   +-- plan/
|   |   |   +-- MVRelation.scala             # MV scan leaf node
|   |   |   +-- MVStrategy.scala             # Physical execution strategy
|   |   |   +-- logical/
|   |   |       +-- CreateMaterializedViewCommand.scala
|   |   |       +-- AlterMaterializedViewCommand.scala
|   |   |       +-- DropMaterializedViewCommand.scala
|   |   |       +-- RefreshMaterializedViewCommand.scala
|   |   |       +-- ShowMaterializedViewsCommand.scala
|   |   +-- rewrite/
|   |       +-- MaterializedViewRewriteRule.scala  # Top-level optimizer rule
|   |       +-- spjg/
|   |       |   +-- SPJGRewriter.scala              # Core SPJG matching
|   |       |   +-- EquivalenceClasses.scala         # Union-find equivalence
|   |       |   +-- MatchModality.scala              # Match mode classification
|   |       |   +-- CompensationPredicates.scala     # Compensation computation
|   |       |   +-- AggregateRollup.scala            # Aggregate rollup logic
|   |       |   +-- TableMappingGenerator.scala      # Table/attribute mapping
|   |       +-- util/
|   |           +-- ExpressionLineage.scala          # Expression tracing
|   |           +-- PredicateSplitter.scala          # Predicate classification
|   +-- resources/
|       +-- reference.conf
+-- test/scala/org/apache/spark/sql/mv/
    +-- catalog/MVCatalogSuite.scala
    +-- parser/MVSqlParserSuite.scala
    +-- plan/logical/
    |   +-- CreateMVCommandSuite.scala
    |   +-- AlterMVCommandSuite.scala
    |   +-- DropMVCommandSuite.scala
    |   +-- RefreshMVCommandSuite.scala
    |   +-- ShowMVCommandSuite.scala
    +-- rewrite/
        +-- EquivalenceClassesSuite.scala
        +-- PredicateSplitterSuite.scala
        +-- CompensationPredicatesSuite.scala
        +-- AggregateRollupSuite.scala
        +-- MatchModalitySuite.scala
        +-- SPJGRewriterSuite.scala
        +-- MaterializedViewRewriteRuleSuite.scala
```

## Running Tests

```bash
# Run all tests
mvn test

# Run a specific test suite
mvn test -Dtest=org.apache.spark.sql.mv.catalog.MVCatalogSuite

# Skip tests during build
mvn clean package -DskipTests
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `spark.sql.extensions` | (none) | Must include `org.apache.spark.sql.mv.MVSessionExtensions` |

## Technology Stack

| Component | Version | Notes |
|-----------|---------|-------|
| Apache Spark | 3.3.2 | Provided scope |
| Scala | 2.12.15 | Compatible with Spark 3.3.2 |
| ANTLR4 | 4.9.3 | Matches Spark's internal ANTLR version |
| ScalaTest | 3.2.15 | Unit testing |
| Maven | 3.6+ | Build tool |

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Rewriting algorithm | Calcite MaterializedViewRule | Targeted for SPJG, extensible match modalities |
| ANTLR grammar | Independent file, delegation pattern | Decoupled from Spark internals, query parsed by Spark |
| Catalog storage | In-memory + pluggable interface | Easy development/testing, extensible for persistence |
| Default format | Parquet | Best columnar storage support in Spark |
| Extension mechanism | `spark.sql.extensions` | No Spark source modification required |

## LLM
[GLM](https://bigmodel.cn/)

## License

Apache License 2.0
