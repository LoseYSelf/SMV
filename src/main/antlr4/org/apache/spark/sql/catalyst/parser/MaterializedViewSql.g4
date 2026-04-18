grammar MaterializedViewSql;

// ========================
// Entry Point
// ========================

singleStatement
    : statement EOF
    ;

statement
    : createMaterializedView
    | alterMaterializedView
    | dropMaterializedView
    | refreshMaterializedView
    | showMaterializedViews
    ;

// ========================
// CREATE MATERIALIZED VIEW
// AS_QUERY_TEXT captures everything after AS for delegation to Spark parser
// ========================

createMaterializedView
    : CREATE (OR REPLACE)? MATERIALIZED VIEW (IF NOT EXISTS)? mvIdentifier
      (COMMENT STRING)?
      (USING mvIdentifier)?
      (OPTIONS propertyList)?
      (PARTITIONED BY parenIdentifierList)?
      (TBLPROPERTIES propertyList)?
      AS AS_QUERY_TEXT
    ;

// ========================
// ALTER MATERIALIZED VIEW
// ========================

alterMaterializedView
    : ALTER MATERIALIZED VIEW mvIdentifier RENAME TO mvIdentifier    #alterMVRename
    | ALTER MATERIALIZED VIEW mvIdentifier SET TBLPROPERTIES propertyList  #alterMVSetProperties
    | ALTER MATERIALIZED VIEW mvIdentifier UNSET TBLPROPERTIES (IF EXISTS)? parenIdentifierList  #alterMVUnsetProperties
    ;

// ========================
// DROP MATERIALIZED VIEW
// ========================

dropMaterializedView
    : DROP MATERIALIZED VIEW (IF EXISTS)? mvIdentifier
    ;

// ========================
// REFRESH MATERIALIZED VIEW
// ========================

refreshMaterializedView
    : REFRESH MATERIALIZED VIEW mvIdentifier
    ;

// ========================
// SHOW MATERIALIZED VIEWS
// ========================

showMaterializedViews
    : SHOW MATERIALIZED VIEWS (FROM | IN)? identifier
    ;

// ========================
// Common Rules
// ========================

mvIdentifier
    : identifier (DOT identifier)?
    ;

identifier
    : IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    | nonReserved
    ;

nonReserved
    : CREATE | OR | REPLACE | MATERIALIZED | VIEW | IF | NOT | EXISTS
    | COMMENT | USING | OPTIONS | PARTITIONED | BY | TBLPROPERTIES | AS
    | ALTER | RENAME | TO | SET | UNSET | DROP | REFRESH | SHOW | VIEWS
    | FROM | IN
    ;

parenIdentifierList
    : LPAREN identifier (COMMA identifier)* RPAREN
    ;

propertyList
    : LPAREN property (COMMA property)* RPAREN
    ;

property
    : propertyKey EQ propertyValue
    ;

propertyKey
    : identifier
    | STRING
    ;

propertyValue
    : INTEGER_VALUE
    | STRING
    | identifier
    ;

// ========================
// Keywords (case-insensitive via lexer)
// ========================

CREATE: 'CREATE';
OR: 'OR';
REPLACE: 'REPLACE';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
IF: 'IF';
NOT: 'NOT';
EXISTS: 'EXISTS';
COMMENT: 'COMMENT';
USING: 'USING';
OPTIONS: 'OPTIONS';
PARTITIONED: 'PARTITIONED';
BY: 'BY';
TBLPROPERTIES: 'TBLPROPERTIES';
AS: 'AS';
ALTER: 'ALTER';
RENAME: 'RENAME';
TO: 'TO';
SET: 'SET';
UNSET: 'UNSET';
DROP: 'DROP';
REFRESH: 'REFRESH';
SHOW: 'SHOW';
VIEWS: 'VIEWS';
FROM: 'FROM';
IN: 'IN';

// ========================
// Symbols
// ========================

EQ: '=';
LPAREN: '(';
RPAREN: ')';
COMMA: ',';
DOT: '.';

// ========================
// Literals & Identifiers
// ========================

STRING: '\'' ( ~('\'' | '\\') | ('\\' .) )* '\'';
INTEGER_VALUE: DIGIT+;
IDENTIFIER: (LETTER | '_') (LETTER | DIGIT | '_')*;
BACKQUOTED_IDENTIFIER: '`' ( ~'`' | '``' )* '`';

// ========================
// Query Text Capture
// Matches everything after the AS keyword until EOF.
// The MVSqlParser strips the DDL prefix and extracts this text
// to delegate to Spark's native SQL parser.
// ========================

AS_QUERY_TEXT: .+?;

// ========================
// Fragments
// ========================

fragment LETTER: [a-zA-Z];
fragment DIGIT: [0-9];

// ========================
// Whitespace & Comments
// ========================

WS: [ \t\r\n]+ -> skip;
SIMPLE_COMMENT: '--' ~[\r\n]* -> skip;
BRACKETED_COMMENT: '/*' .*? '*/' -> skip;