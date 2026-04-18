# Spark Materialized View (SMV)

[English](README.md) | [中文](README_CN.md)

## 概述

**Spark Materialized View (SMV)** 是一个针对 Apache Spark 3.3.2 的查询改写插件，提供了一流的物化视图支持。它能够自动利用预计算的物化视图来改写查询，在不修改用户查询的前提下大幅加速分析型工作负载。

改写算法参考了 Apache Calcite 的 `MaterializedViewRule`，并在 Spark Catalyst 优化器上实现，支持 SPJG（Select-Project-Join-GroupBy）查询模式，包含三种匹配模式和聚合上卷能力。

## 功能特性

- **DDL 支持**：完整的 `CREATE / ALTER / DROP / REFRESH / SHOW MATERIALIZED VIEW` 语法，使用 ANTLR4 语法定义
- **自动查询改写**：优化器规则透明地将查询改写为使用物化视图
- **三种匹配模式**：
  - `COMPLETE`（完全匹配）— 查询与 MV 引用相同的表集
  - `QUERY_PARTIAL`（查询部分匹配）— MV 包含额外的表（需验证基数保持的 Join）
  - `VIEW_PARTIAL`（视图部分匹配）— 查询包含额外的表（需在 MV 上方补充 Join）
- **聚合上卷**：SUM、MIN、MAX 直接复用；COUNT → SUM0；AVG → SUM/COUNT 分解
- **补偿谓词**：等值补偿（等价类覆盖）和残余谓词补偿
- **Spark Extension 注入**：通过 `spark.sql.extensions` 接入，无需修改 Spark 源码
- **内存目录**：默认使用 ConcurrentHashMap 的元数据存储，提供可插拔的持久化接口
- **Parquet 存储**：默认列式存储格式

## 架构

```
SparkSession
  │
  ├── MVSessionExtensions（注入入口）
  │     ├── MVSqlParser           — 自定义解析器（处理 MV DDL，非 MV SQL 委托给 Spark）
  │     ├── MaterializedViewRewriteRule — 查询改写优化器规则
  │     └── MVStrategy            — DDL 命令执行策略
  │
  ├── 解析层
  │     ├── MaterializedViewSql.g4  — ANTLR4 语法（仅 DDL，查询部分委托 Spark 解析）
  │     ├── MVAstBuilder            — 解析树 → LogicalPlan 访问器
  │     └── MVSqlParser             — 委托模式解析器
  │
  ├── 目录层
  │     ├── MVCatalog               — 元数据管理（增删改查 + 状态）
  │     ├── MVInMemoryCatalogStore  — 默认 ConcurrentHashMap 存储
  │     └── MVExternalCatalog       — 可插拔持久化接口
  │
  ├── 模型层
  │     ├── MVDefinition            — 核心定义（计划、模式、状态、存储）
  │     ├── MVIdentifier            — 限定名（db.name）
  │     └── MVState                 — 生命周期（FRESH / STALE / REFRESHING / DISABLED）
  │
  ├── 改写层
  │     ├── MaterializedViewRewriteRule — 顶层优化器规则
  │     └── SPJG 改写引擎
  │           ├── SPJGRewriter           — 核心匹配算法
  │           ├── EquivalenceClasses     — 并查集等价类追踪
  │           ├── MatchModality          — COMPLETE / QUERY_PARTIAL / VIEW_PARTIAL
  │           ├── CompensationPredicates — 等值 + 残余补偿
  │           ├── AggregateRollup        — 聚合函数上卷逻辑
  │           ├── TableMappingGenerator  — 查询到 MV 的表/属性映射
  │           ├── PredicateSplitter      — 等值/残余谓词分离
  │           └── ExpressionLineage      — 表达式溯源追踪
  │
  └── 计划层
        ├── MVRelation              — 改写计划的叶扫描节点
        └── Logical Commands        — Create/Alter/Drop/Refresh/Show MV
```

## 快速开始

### 1. 构建

```bash
mvn clean package -DskipTests
```

环境要求：
- Java 8+
- Maven 3.6+
- Scala 2.12.15（Maven 自动处理）
- Spark 3.3.2（provided 作用域）

### 2. 启用扩展

将 JAR 包添加到 Spark 类路径并配置扩展：

```bash
# 通过 spark-submit
spark-submit \
  --jars spark-materialized-view-0.1.0.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.mv.MVSessionExtensions \
  your-app.jar

# 通过 spark-shell
spark-shell \
  --jars spark-materialized-view-0.1.0.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.mv.MVSessionExtensions
```

或编程方式：

```scala
import org.apache.spark.sql.mv.MVSessionExtensions

val spark = SparkSession.builder()
  .appName("MV 演示")
  .withExtensions(new MVSessionExtensions())
  .getOrCreate()
```

### 3. 创建物化视图

```sql
-- 简单 MV
CREATE MATERIALIZED VIEW mv_sales_by_region
AS SELECT region, SUM(amount) AS total_amount
   FROM sales GROUP BY region;

-- 带选项
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_stats
USING parquet
OPTIONS ('compression'='snappy')
TBLPROPERTIES ('refresh.interval'='1h')
COMMENT '每日统计聚合'
AS SELECT date, category, COUNT(*) AS cnt, AVG(price) AS avg_price
   FROM orders GROUP BY date, category;
```

### 4. 查询改写（自动）

当物化视图存在时，优化器自动改写兼容的查询：

```sql
-- 原始查询（扫描基表）
SELECT region, SUM(amount) FROM sales GROUP BY region;

-- 自动改写为使用 MV（显著加速）
-- 优化器将扫描替换为指向 mv_sales_by_region 的 MVRelation
```

带补偿谓词：

```sql
-- MV: SELECT region, SUM(amount) FROM sales GROUP BY region
-- 带额外过滤的查询:
SELECT region, SUM(amount) FROM sales WHERE region = 'US' GROUP BY region;

-- 改写为: SELECT * FROM mv_sales_by_region WHERE region = 'US'
```

带聚合上卷：

```sql
-- MV: SELECT date, category, COUNT(*), AVG(price) FROM orders GROUP BY date, category
-- 更粗粒度分组的查询:
SELECT category, SUM(COUNT), SUM(AVG_PRICE * COUNT) / SUM(COUNT) FROM orders GROUP BY category;

-- 改写为: 用更少的分组列对 MV 数据重新聚合
```

### 5. 管理物化视图

```sql
-- 刷新数据
REFRESH MATERIALIZED VIEW mv_sales_by_region;

-- 重命名
ALTER MATERIALIZED VIEW mv_sales_by_region RENAME TO mv_regional_sales;

-- 设置属性
ALTER MATERIALIZED VIEW mv_regional_sales SET TBLPROPERTIES ('retention'='30d');

-- 取消属性
ALTER MATERIALIZED VIEW mv_regional_sales UNSET TBLPROPERTIES ('retention');

-- 删除
DROP MATERIALIZED VIEW IF EXISTS mv_regional_sales;

-- 列出视图
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS FROM my_database;
```

## DDL 语法参考

### CREATE MATERIALIZED VIEW

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] mv_identifier
  [COMMENT '描述']
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

## 改写算法

### SPJG 匹配流程

```
查询计划
  │
  ├─ 1. 验证仅含 SPJG 操作符（Filter/Project/Join/Aggregate/Scan）
  │
  ├─ 2. 提取元数据（表引用、谓词、等价类）
  │
  ├─ 3. 判定匹配模式
  │     ├─ COMPLETE:      queryTables == viewTables
  │     ├─ QUERY_PARTIAL: viewTables ⊃ queryTables（MV 多余 Join 需为基数保持）
  │     ├─ VIEW_PARTIAL:  queryTables ⊃ viewTables（需补偿 Join）
  │     └─ NO_MATCH:      表集不兼容
  │
  ├─ 4. 生成表映射（属性级双射）
  │
  ├─ 5. 计算补偿谓词
  │     ├─ 等值补偿: 查询等价类 ⊆ 映射后的 MV 等价类
  │     └─ 残余补偿: 查询残余谓词不被映射后的 MV 残余谓词蕴含
  │
  ├─ 6. 计算聚合上卷（如适用）
  │
  └─ 7. 构建改写计划: MVRelation + Filter + Aggregate + Project + Join
```

### 聚合上卷规则

| 查询聚合函数 | MV 中存储     | 上卷方式                           |
|-------------|---------------|------------------------------------|
| `SUM(x)`   | `SUM(x)`      | 直接复用：`SUM(mv_sum)`            |
| `MIN(x)`   | `MIN(x)`      | 直接复用：`MIN(mv_min)`            |
| `MAX(x)`   | `MAX(x)`      | 直接复用：`MAX(mv_max)`            |
| `COUNT(x)` | `COUNT(x)`    | `COALESCE(SUM(mv_count), 0)`       |
| `AVG(x)`   | `SUM(x)+COUNT(x)` | `SUM(mv_sum) / SUM(mv_count)` |

查询分组列必须是 MV 分组列的子集。

### MV 生命周期状态

| 状态         | 可改写 | 描述                           |
|-------------|--------|-------------------------------|
| `FRESH`     | 是     | 数据最新                       |
| `STALE`     | 否     | 基表已修改，数据过期            |
| `REFRESHING`| 否     | 正在刷新                       |
| `DISABLED`  | 否     | 改写已显式禁用                  |

## 项目结构

```
src/
├── main/
│   ├── antlr4/org/apache/spark/sql/catalyst/parser/
│   │   └── MaterializedViewSql.g4          # ANTLR4 DDL 语法
│   ├── scala/org/apache/spark/sql/mv/
│   │   ├── MVSessionExtensions.scala        # 扩展注入入口
│   │   ├── parser/
│   │   │   ├── MVSqlParser.scala            # 委托解析器
│   │   │   └── MVAstBuilder.scala           # 解析树 → LogicalPlan
│   │   ├── catalog/
│   │   │   ├── MVCatalog.scala              # 元数据管理
│   │   │   ├── MVInMemoryCatalogStore.scala # 内存存储
│   │   │   └── MVExternalCatalog.scala      # 持久化接口
│   │   ├── model/
│   │   │   ├── MVDefinition.scala           # 核心模型类
│   │   │   └── MVProperty.scala             # 属性模型
│   │   ├── plan/
│   │   │   ├── MVRelation.scala             # MV 扫描叶节点
│   │   │   ├── MVStrategy.scala             # 物理执行策略
│   │   │   └── logical/
│   │   │       ├── CreateMaterializedViewCommand.scala
│   │   │       ├── AlterMaterializedViewCommand.scala
│   │   │       ├── DropMaterializedViewCommand.scala
│   │   │       ├── RefreshMaterializedViewCommand.scala
│   │   │       └── ShowMaterializedViewsCommand.scala
│   │   └── rewrite/
│   │       ├── MaterializedViewRewriteRule.scala  # 顶层优化器规则
│   │       ├── spjg/
│   │       │   ├── SPJGRewriter.scala              # 核心 SPJG 匹配
│   │       │   ├── EquivalenceClasses.scala         # 并查集等价类
│   │       │   ├── MatchModality.scala              # 匹配模式分类
│   │       │   ├── CompensationPredicates.scala     # 补偿计算
│   │       │   ├── AggregateRollup.scala            # 聚合上卷逻辑
│   │       │   └── TableMappingGenerator.scala      # 表/属性映射
│   │       └── util/
│   │           ├── ExpressionLineage.scala          # 表达式溯源
│   │           └── PredicateSplitter.scala          # 谓词分类
│   └── resources/
│       └── reference.conf
└── test/scala/org/apache/spark/sql/mv/
    ├── catalog/MVCatalogSuite.scala
    ├── parser/MVSqlParserSuite.scala
    ├── plan/logical/
    │   ├── CreateMVCommandSuite.scala
    │   ├── AlterMVCommandSuite.scala
    │   ├── DropMVCommandSuite.scala
    │   ├── RefreshMVCommandSuite.scala
    │   └── ShowMVCommandSuite.scala
    └── rewrite/
        ├── EquivalenceClassesSuite.scala
        ├── PredicateSplitterSuite.scala
        ├── CompensationPredicatesSuite.scala
        ├── AggregateRollupSuite.scala
        ├── MatchModalitySuite.scala
        ├── SPJGRewriterSuite.scala
        └── MaterializedViewRewriteRuleSuite.scala
```

## 运行测试

```bash
# 运行所有测试
mvn test

# 运行指定测试套件
mvn test -Dtest=org.apache.spark.sql.mv.catalog.MVCatalogSuite

# 构建时跳过测试
mvn clean package -DskipTests
```

## 配置

| 属性 | 默认值 | 描述 |
|------|--------|------|
| `spark.sql.extensions` | （无） | 必须包含 `org.apache.spark.sql.mv.MVSessionExtensions` |

## 技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| Apache Spark | 3.3.2 | provided 作用域 |
| Scala | 2.12.15 | 兼容 Spark 3.3.2 |
| ANTLR4 | 4.9.3 | 与 Spark 内置 ANTLR 版本一致 |
| ScalaTest | 3.2.15 | 单元测试 |
| Maven | 3.6+ | 构建工具 |

## 设计决策

| 决策项 | 选择 | 原因 |
|--------|------|------|
| 改写算法 | Calcite MaterializedViewRule | 针对 SPJG 查询更具针对性，支持多种匹配模式 |
| ANTLR 语法 | 独立文件 + 委托模式 | 与 Spark 内部语法解耦，查询由 Spark 解析 |
| 目录存储 | 内存 + 可插拔接口 | 便于开发测试，支持持久化扩展 |
| 默认格式 | Parquet | Spark 中列式存储支持最好 |
| 扩展机制 | `spark.sql.extensions` | 无需修改 Spark 源码 |

## 大模型
[GLM](https://bigmodel.cn/)

## 许可证

Apache License 2.0
