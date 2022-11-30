<#--

    Copyright (C) 2017-2019 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->
/**
 *   SHOW TABLES
 *   [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
 *   [ ( FROM | IN ) source ]
 *   [ LIKE 'pattern' ]
 */
SqlNode SqlShowTables() :
{
    SqlParserPos pos;
    ReferenceType refType = null;
    SqlIdentifier refValue = null;
    SqlIdentifier source = null;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <TABLES>
    [
        <AT>
        (
            <REF> { refType = ReferenceType.REFERENCE; }
            |
            <REFERENCE> { refType = ReferenceType.REFERENCE; }
            |
            <BRANCH> { refType = ReferenceType.BRANCH; }
            |
            <TAG> { refType = ReferenceType.TAG; }
            |
            <COMMIT> { refType = ReferenceType.COMMIT; }
        )
        { refValue = SimpleIdentifier(); }
    ]
    [
        (<FROM> | <IN>) { source = CompoundIdentifier(); }
    ]
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowTables(pos, refType, refValue, source, likePattern);
    }
}

/**
 *   SHOW VIEWS
 *   [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
 *   [ ( FROM | IN ) source ]
 *   [ LIKE 'pattern' ]
 */
SqlNode SqlShowViews() :
{
    SqlParserPos pos;
    ReferenceType refType = null;
    SqlIdentifier refValue = null;
    SqlIdentifier source = null;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <VIEWS>
    [
        <AT>
        (
            <REF> { refType = ReferenceType.REFERENCE; }
            |
            <REFERENCE> { refType = ReferenceType.REFERENCE; }
            |
            <BRANCH> { refType = ReferenceType.BRANCH; }
            |
            <TAG> { refType = ReferenceType.TAG; }
            |
            <COMMIT> { refType = ReferenceType.COMMIT; }
        )
        { refValue = SimpleIdentifier(); }
    ]
    [
        (<FROM> | <IN>) { source = CompoundIdentifier(); }
    ]
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowViews(pos, refType, refValue, source, likePattern);
    }
}

/**
 * Parses statement
 * SHOW FILES [{FROM | IN} schema]
 */
SqlNode SqlShowFiles() :
{
    SqlParserPos pos = null;
    SqlIdentifier db = null;
}
{
    <SHOW> { pos = getPos(); }
    <FILES>
    [
        (<FROM> | <IN>) { db = CompoundIdentifier(); }
    ]
    {
        return new SqlShowFiles(pos, db);
    }
}


/**
 * Parses statement SHOW {DATABASES | SCHEMAS} [LIKE 'pattern']
 */
SqlNode SqlShowSchemas() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    (<DATABASES> | <SCHEMAS>)
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowSchemas(pos, likePattern);
    }
}

/**
 * Parses statement
 *   { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
SqlNode SqlDescribeTable() :
{
    SqlParserPos pos;
    SqlIdentifier table;
    SqlIdentifier column = null;
}
{
    (<DESCRIBE> | <DESC>) { pos = getPos(); }
    table = CompoundIdentifier()
    (
        column = CompoundIdentifier()
        |
        E()
    )
    {
        return new SqlDescribeTable(pos, table, column);
    }
}

SqlNode SqlUseSchema():
{
    SqlIdentifier schema;
    SqlParserPos pos;
}
{
    <USE> { pos = getPos(); }
    schema = CompoundIdentifier()
    {
        return new SqlUseSchema(pos, schema);
    }
}

/** Parses an optional field list and makes sure no field is a "*". */
SqlNodeList ParseOptionalFieldList(String relType) :
{
    SqlNodeList fieldList;
}
{
    fieldList = ParseRequiredFieldList(relType)
    {
        return fieldList;
    }
    |
    {
        return SqlNodeList.EMPTY;
    }
}
SqlNodeList ParseOptionalFieldListWithMasking(String relType) :
{
    SqlNodeList fieldList;
}
{
    fieldList = ParseRequiredFieldListWithMasking(relType)
    {
        return fieldList;
    }
    |
    {
        return SqlNodeList.EMPTY;
    }
}

/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldList(String relType) :
{
    SqlNodeList fieldList = new SqlNodeList(getPos());
}
{
    <LPAREN>
    SimpleIdentifierCommaList(fieldList.getList())
    <RPAREN>
    {
        for(SqlNode node : fieldList)
        {
            if (((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return fieldList;
    }
}
SqlNodeList ParseRequiredFieldListWithMasking(String relType) :
{
    final Span s;
    final List<SqlNode> fieldList = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ColumnNamesWithMasking(fieldList)
    (
        <COMMA> ColumnNamesWithMasking(fieldList)
    )*
    <RPAREN> {
        for(SqlNode node : fieldList)
        {
            if (((SqlColumnPolicyPair)node).getName().isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return new SqlNodeList(fieldList, s.end(this));
    }
}

void ColumnNamesWithMasking(List<SqlNode> list) :
{
    final SqlIdentifier id;
    SqlPolicy policy = null;
    final Span s = span();
}
{
    id = SimpleIdentifier()
    (
      <MASKING> <POLICY>  policy = Policy()
      {
          list.add(new SqlColumnPolicyPair(s.add(id).end(this), id, policy));
      }
      |
      {
          list.add(new SqlColumnPolicyPair(s.add(id).end(this), id, null));
      }
    )
}

/**
 * Parses a create view or replace existing view statement.
 *   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateOrReplace() :
{
    SqlParserPos pos;
    boolean replace = false;
    SqlIdentifier viewName;
    SqlNode query;
    SqlNodeList fieldList = SqlNodeList.EMPTY;
    SqlIdentifier name;
    SqlNode expression;
    SqlDataTypeSpec returnType;
    boolean ifNotExists = false;
    SqlPolicy policy = null;
    boolean nullable;
}
{
    <CREATE> { pos = getPos(); }
    [ <OR> <REPLACE> { replace = true; } ]
    (
      <FUNCTION>
        [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
        name = CompoundIdentifier()
        fieldList = ParseFunctionFieldList()
      <RETURNS>
        returnType = DataType()
        nullable = NullableOptDefaultTrue()
      <RETURN> {
        expression = OrderedQueryOrExpr(ExprContext.ACCEPT_ALL);
        return new SqlCreateFunction(pos, SqlLiteral.createBoolean(replace, SqlParserPos.ZERO), name, fieldList, new SqlComplexDataTypeSpec(returnType.withNullable(nullable)), expression, SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO));
      }
      |
      (<VIEW>|<VDS>)
      viewName = CompoundIdentifier()
      fieldList = ParseOptionalFieldListWithMasking("View")
      [
          <ROW><ACCESS><POLICY>
          {
              policy = Policy();
          }
      ]
      <AS> { pos = pos.plus(getPos()); }
      query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
      {
          return new SqlCreateView(pos, viewName, fieldList, query, replace, policy);
      }
    )
}

/**
*  DROP UDF
*/
SqlNode SqlDropFunction() :
{
  SqlParserPos pos;
  SqlLiteral ifExists = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier name;
}
{
  <DROP> { pos = getPos(); }
  <FUNCTION>
  [ <IF> <EXISTS> { ifExists = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); } ]
  {
    name = CompoundIdentifier();
    return new SqlDropFunction(pos, ifExists, name);
  }
}

/**
*  DESCRIBE UDF
*/
SqlNode SqlDescribeFunction() :
{
  SqlParserPos pos;
  SqlIdentifier function;
}
{
  (<DESCRIBE> | <DESC>) { pos = getPos(); }
  <FUNCTION>
  {
    function = CompoundIdentifier();
    return new SqlDescribeFunction(pos, function);
  }
}

/**
 *   SHOW FUNCTIONS
 *   [ LIKE 'pattern' ]
 */
SqlNode SqlShowFunctions() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <FUNCTIONS>
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowFunctions(pos, likePattern);
    }
}

SqlNodeList ParseFunctionFieldList() :
{
  SqlNodeList fieldList = new SqlNodeList(getPos());
}
{
  <LPAREN>
    FieldFunctionTypeCommaList(fieldList.getList())
  <RPAREN>
  {
    return fieldList;
  }
}

SqlNodeList FunctionKeyValuePair() :
{
  SqlNodeList pair = new SqlNodeList(getPos());
  SqlIdentifier name;
  SqlDataTypeSpec type;
  boolean nullable;
}
{
  name = SimpleIdentifier() { pair.add(name); }
  type = DataType()
  nullable = NullableOptDefaultTrue()
    {
      pair.add(new SqlComplexDataTypeSpec(type.withNullable(nullable)));
      return pair;
    }
}

void FieldFunctionTypeCommaList(List<SqlNode> list) :
{
  SqlNodeList pair;
}
{
  (
    pair = FunctionKeyValuePair() { list.add(pair); }
    (
      <COMMA> pair = FunctionKeyValuePair() {
        list.add(pair);
      }
    )*
  )?
}

/**
 * Parses a drop view or drop view if exists statement.
 * DROP VIEW [IF EXISTS] view_name;
 */
SqlNode SqlDropView() :
{
    SqlParserPos pos;
    boolean viewExistenceCheck = false;
}
{
    <DROP> { pos = getPos(); }
    ( <VIEW> | <VDS> )
    [ <IF> <EXISTS> { viewExistenceCheck = true; } ]
    {
        return new SqlDropView(pos, CompoundIdentifier(), viewExistenceCheck);
    }
}

SqlNodeList TableElementListWithMasking() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElementWithMasking(list)
    (
        <COMMA> TableElementWithMasking(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElementWithMasking(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    SqlPolicy policy = null;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
          <MASKING> <POLICY>  policy = Policy()
          {
              list.add(
                      new SqlColumnDeclaration(s.add(id).end(this), new SqlColumnPolicyPair(id.getParserPosition(), id, policy),
                      new SqlComplexDataTypeSpec(type.withNullable(nullable)), null));
          }
          |
          {
              list.add(
                      new SqlColumnDeclaration(s.add(id).end(this), new SqlColumnPolicyPair(id.getParserPosition(), id, null),
                      new SqlComplexDataTypeSpec(type.withNullable(nullable)), null));
          }
        )
        |
        (
          <MASKING> <POLICY>  policy = Policy()
          {
            list.add(new SqlColumnPolicyPair(id.getParserPosition(), id, policy));
          }
          |
          {
            list.add(new SqlColumnPolicyPair(id.getParserPosition(), id, null));
          }
        )
    )
    |
    (
      id = SimpleIdentifier()
      (
          <MASKING> <POLICY>  policy = Policy()
          {
            list.add(new SqlColumnPolicyPair(id.getParserPosition(), id, policy));
          }
          |
          {
            list.add(new SqlColumnPolicyPair(id.getParserPosition(), id, null));
          }
      )
    )
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        {
            list.add(
                    new SqlColumnDeclaration(s.add(id).end(this), new SqlColumnPolicyPair(id.getParserPosition(), id, null),
                    new SqlComplexDataTypeSpec(type.withNullable(nullable)), null));
        }
        |
        { list.add(id); }
    )
    |
    id = SimpleIdentifier() {
        list.add(id);
    }
}

/**
 * Parses a partition transform:
 *
 *   partition_transform:
 *       <column_name>
 *     | <transform_name> ( [ <literal_arg>, ... ] <column_name> )
 */
SqlPartitionTransform ParsePartitionTransform() :
{
    SqlIdentifier id;
    SqlIdentifier columnName;
    List<SqlLiteral> argList = Lists.newArrayList();
    SqlNode arg;
    Span s;
    Token token;
}
{
    (
        id = SimpleIdentifier() { s = span(); }
        [
            <LPAREN>
            (
                arg = Literal() {
                    argList.add((SqlLiteral) arg);
                }
                <COMMA>
            )*
            columnName = SimpleIdentifier()
            <RPAREN> {
                return new SqlPartitionTransform(columnName, id, argList, s.end(this));
            }
        ]
        {
            return new SqlPartitionTransform(id, id.getParserPosition());
        }
    |
        (token = <YEAR> | token = <MONTH> | token = <HOUR> | token = <DAY> | token = <TRUNCATE> | token = <IDENTITY>) {
            s = span();
            id = new SqlIdentifier(token.toString(), getPos());
        }
        <LPAREN>
        (
            arg = Literal() {
                argList.add((SqlLiteral) arg);
            }
            <COMMA>
        )*
        columnName = SimpleIdentifier()
        <RPAREN> {
            return new SqlPartitionTransform(columnName, id, argList, s.end(this));
        }
    )
}

/** Parses a partition transform list */
SqlNodeList ParsePartitionTransformList() :
{
    SqlNodeList transformList = new SqlNodeList(getPos());
    SqlPartitionTransform transform;
}
{
    <LPAREN>
    transform = ParsePartitionTransform() {
        transformList.add(transform);
    }
    (
        <COMMA> transform = ParsePartitionTransform() {
            transformList.add(transform);
        }
    )*
    <RPAREN> {
        return transformList;
    }
}


/**
 * Parses a CTAS statement.
 * CREATE TABLE tblname [ (field1, field2, ...) ]
 *       [ (STRIPED, HASH, ROUNDROBIN) PARTITION BY (field1, field2, ..) ]
 *       [ DISTRIBUTE BY (field1, field2, ..) ]
 *       [ LOCALSORT BY (field1, field2, ..) ]
 *       [ STORE AS (opt1 => val1, opt2 => val3, ...) ]
 *       [ LOCATION location]
 *       [ WITH SINGLE WRITER ]
 *       [ AS select_statement. ]
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    List<SqlNode> formatList = new ArrayList();
    SqlNodeList formatOptions;
    SqlNode location = null;
    PartitionDistributionStrategy partitionDistributionStrategy;
    SqlNodeList partitionTransformList;
    SqlNodeList distributeFieldList;
    SqlNodeList sortFieldList;
    SqlLiteral singleWriter;
    SqlNode query;
    boolean ifNotExists = false;
    SqlPolicy policy = null;
}
{
    {
        partitionTransformList = SqlNodeList.EMPTY;
        distributeFieldList = SqlNodeList.EMPTY;
        sortFieldList =  SqlNodeList.EMPTY;
        formatOptions = SqlNodeList.EMPTY;
        location = null;
        singleWriter = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
        fieldList = SqlNodeList.EMPTY;
        policy = null;
    }
    <CREATE> { pos = getPos(); }
    <TABLE>
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    tblName = CompoundIdentifier()
    [ fieldList = TableElementListWithMasking() ]
    (
        (
            <STRIPED> {
                partitionDistributionStrategy = PartitionDistributionStrategy.STRIPED;
            }
        |
            <HASH> {
                partitionDistributionStrategy = PartitionDistributionStrategy.HASH;
            }
        |
            <ROUNDROBIN> {
                partitionDistributionStrategy = PartitionDistributionStrategy.ROUND_ROBIN;
            }
        )?
        <PARTITION> <BY>
        partitionTransformList = ParsePartitionTransformList()
    )?
    (   <DISTRIBUTE> <BY>
        distributeFieldList = ParseRequiredFieldList("Distribution")
    )?
    (   <LOCALSORT> <BY>
        sortFieldList = ParseRequiredFieldList("Sort")
    )?
    (
            <LOCATION> { location = StringLiteral(); }
    )?
    [
        <STORE> <AS>
        <LPAREN>
            Arg0(formatList, ExprContext.ACCEPT_CURSOR)
            (
                <COMMA>
                Arg(formatList, ExprContext.ACCEPT_CURSOR)
            )*
        <RPAREN>
        {
            formatOptions = new SqlNodeList(formatList, getPos());
        }
    ]
    [
        <WITH><SINGLE><WRITER>
        {
            singleWriter = SqlLiteral.createBoolean(true, getPos());
        }
    ]
    [
        <ROW><ACCESS><POLICY>
        {
            policy = Policy();
        }
    ]
        (
            (
                <AS>
                query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
                {
                    return new SqlCreateTable(pos, tblName, fieldList, ifNotExists, partitionDistributionStrategy,
                        partitionTransformList, formatOptions, location, singleWriter, sortFieldList,
                        distributeFieldList, policy, query);
                }
            )
            |
            (
                {
                    return new SqlCreateEmptyTable(pos, tblName, fieldList, ifNotExists, partitionDistributionStrategy,
                        partitionTransformList, formatOptions, location, singleWriter, sortFieldList,
                        distributeFieldList, policy);
                }
            )
        )
}

/**
* Parses a insert table or drop table if exists statement.
* INSERT INTO table_name select_statement;
*/
SqlNode SqlInsertTable() :
{
  SqlParserPos pos;
  SqlIdentifier tblName;
  SqlNode query;
  SqlNodeList fieldList;
}
{
  {
    fieldList = SqlNodeList.EMPTY;
  }

  <INSERT> { pos = getPos(); }
  <INTO>
    tblName = CompoundIdentifier()
    [ fieldList = TableElementList() ]
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
      return new SqlInsertTable(pos, tblName, query, fieldList);
    }
}

/**
* Parses a delete from table
* DELETE FROM targetTable [ WHERE condition ];
*/
SqlNode SqlDeleteFromTable() :
{
  SqlParserPos pos;
  SqlIdentifier targetTable;
  SqlIdentifier alias = null;
  SqlNode sourceTableRef = null;
  SqlNode condition;
}
{
  <DELETE> { pos = getPos(); }
  <FROM>
    targetTable = CompoundIdentifier()
    [ [ <AS> ] alias = SimpleIdentifier() ]
    [ <USING> sourceTableRef = FromClause() ]
    condition = WhereOpt()
    {
      return new SqlDeleteFromTable(pos, targetTable, condition, alias, sourceTableRef);
    }
}

/**
* Parses a update table statement
* UPDATE targetTable
* SET <id> = <exp> [, <id> = <exp> ... ]
* [ WHERE condition ];
*/
SqlNode SqlUpdateTable() :
{
  SqlIdentifier targetTable;
  SqlIdentifier alias = null;
  SqlNode sourceTableRef = null;
  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  SqlNode condition;
  SqlIdentifier id;
  SqlNode exp;
  final Span s;
}
{
  <UPDATE> { s = span(); }
  targetTable = CompoundIdentifier() {
      targetColumnList = new SqlNodeList(s.pos());
      sourceExpressionList = new SqlNodeList(s.pos());
  }
  [ [ <AS> ] alias = SimpleIdentifier() ]
  <SET> id = SimpleIdentifier() {
      targetColumnList.add(id);
  }
  <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
      sourceExpressionList.add(exp);
  }
  (
      <COMMA>
      id = SimpleIdentifier()
      {
          targetColumnList.add(id);
      }
      <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY)
      {
          sourceExpressionList.add(exp);
      }
  )*
  [ <FROM> sourceTableRef = FromClause() ]
  condition = WhereOpt()
  {
      return new SqlUpdateTable(s.addAll(targetColumnList)
          .addAll(sourceExpressionList).addIf(condition).pos(), targetTable,
          targetColumnList, sourceExpressionList, condition, alias, sourceTableRef);
  }
}

/**
 * Parses a MERGE statement.
 */
SqlNode SqlMergeIntoTable() :
{
    SqlIdentifier table;
    SqlIdentifier alias = null;
    SqlNode sourceTableRef;
    SqlNode condition;
    SqlUpdate updateCall = null;
    SqlInsert insertCall = null;
    final Span s;
}
{
    <MERGE> { s = span(); } <INTO> table = CompoundIdentifier()
    [ [ <AS> ] alias = SimpleIdentifier() ]
    <USING> sourceTableRef = TableRef()
    <ON> condition = Expression(ExprContext.ACCEPT_SUB_QUERY)
    (
        LOOKAHEAD(2)
        updateCall = DremioWhenMatchedClause(table, alias)
        [ insertCall = DremioWhenNotMatchedClause(table) ]
    |
        insertCall = DremioWhenNotMatchedClause(table)
    )
    {
        return new SqlMergeIntoTable(s.addIf(updateCall).addIf(insertCall).pos(), table,
            condition, sourceTableRef, updateCall, insertCall, alias);
    }
}

/**
 * Dremio version of WhenMatchedClause.
 * It returns Dremio's SqlUpdateTable, which contains extended system columns (i.e., filePath and rowIndex)
 */
SqlUpdate DremioWhenMatchedClause(SqlNode table, SqlIdentifier alias) :
{
    SqlIdentifier id;
    final Span s;
    final SqlNodeList updateColumnList = new SqlNodeList(SqlParserPos.ZERO);
    SqlNode exp;
    final SqlNodeList updateExprList = new SqlNodeList(SqlParserPos.ZERO);
    SqlNode updateSource = null;
}
{
    <WHEN> { s = span(); } <MATCHED> <THEN>
    <UPDATE> <SET>
    (
        <STAR> { updateSource = SqlIdentifier.star(getPos()); }
   |
        id = SimpleIdentifier() {
            updateColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            updateExprList.add(exp);
        }
        (
            <COMMA>
            id = SimpleIdentifier() {
                updateColumnList.add(id);
            }
            <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                updateExprList.add(exp);
            }
        )*
     )
    {
        return new SqlUpdateTable(s.addAll(updateExprList).pos(), table,
            updateColumnList, updateExprList, null, alias, updateSource);
    }
}

/**
 * Dremio version of DremioWhenNotMatchedClause.
 * It returns Dremio's SqlInsertTable, which contains extended system columns (i.e., filePath and rowIndex)
 */
SqlInsert DremioWhenNotMatchedClause(SqlIdentifier table) :
{
    final Span insertSpan, valuesSpan;
    final List<SqlLiteral> keywords = Lists.newArrayList();
    final SqlNodeList keywordList;
    SqlNodeList insertColumnList = null;
    SqlNode rowConstructor;
    SqlNode insertSource;
}
{
    <WHEN> <NOT> <MATCHED> <THEN> <INSERT> {
        insertSpan = span();
    }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, insertSpan.end(this));
    }
    (
        <STAR> { insertSource = SqlIdentifier.star(getPos()); }
    |
        [
            LOOKAHEAD(2)
            insertColumnList = ParenthesizedSimpleIdentifierList()
        ]
        [ <LPAREN> ]
        <VALUES> { valuesSpan = span(); }
        rowConstructor = RowConstructor()
        [ <RPAREN> ]
        {
            // TODO zfong 5/26/06: note that extra parentheses are accepted above
            // around the VALUES clause as a hack for unparse, but this is
            // actually invalid SQL; should fix unparse
            insertSource = SqlStdOperatorTable.VALUES.createCall(
                valuesSpan.end(this), rowConstructor);
        }
    )
    {
        return new SqlInsertTable(insertSpan.end(this),  table, insertSource, insertColumnList);
    }
}

/**
 * Parses a drop table or drop table if exists statement.
 * DROP TABLE [IF EXISTS] table_name;
 */
SqlNode SqlDropTable() :
{
    SqlParserPos pos;
    boolean tableExistenceCheck = false;
}
{
    <DROP> { pos = getPos(); }
    <TABLE>
    [ <IF> <EXISTS> { tableExistenceCheck = true; } ]
    {
        return new SqlDropTable(pos, CompoundIdentifier(), tableExistenceCheck);
    }
}

/**
 * Parses a truncate table statement.
 * TRUNCATE [TABLE] [IF EXISTS] table_name;
 */
SqlNode SqlTruncateTable() :
{
    SqlParserPos pos;
    boolean tableExistenceCheck = false;
    boolean tableKeywordPresent = false;
}
{
    <TRUNCATE> { pos = getPos(); }
    [ <TABLE> { tableKeywordPresent = true; } ]
    [ <IF> <EXISTS> { tableExistenceCheck = true; } ]
    {
        return new SqlTruncateTable(pos, CompoundIdentifier(), tableExistenceCheck, tableKeywordPresent);
    }
}

/**
 * Parses a $REFRESH REFLECTION statement
 *   $REFRESH REFLECTION reflectionId AS materializationId
 */
SqlNode SqlRefreshReflection() :
{
    SqlParserPos pos;
    SqlNode reflectionId;
    SqlNode materializationId;
}
{
    <REFRESH> { pos = getPos(); }
    <REFLECTION>
    { reflectionId = StringLiteral(); }
    <AS>
    { materializationId = StringLiteral(); }
    {
        return new SqlRefreshReflection(pos, reflectionId, materializationId);
    }
}

/**
 * Parses a LOAD MATERIALIZATION statement
 *   $LOAD MATERIALIZATION METADATA materialization_path
 */
SqlNode SqlLoadMaterialization() :
{
    SqlParserPos pos;
    SqlIdentifier materializationPath;
}
{
    <LOAD> { pos = getPos(); }
    <MATERIALIZATION> <METADATA>
    { materializationPath = CompoundIdentifier(); }
    {
        return new SqlLoadMaterialization(pos, materializationPath);
    }
}


/**
 * Parses a COMPACT REFRESH statement
 *   $COMPACT MATERIALIZATION materialization_path AS materializationId
 */
SqlNode SqlCompactMaterialization() :
{
    SqlParserPos pos;
    SqlIdentifier materializationPath;
    SqlNode newMaterializationId;
}
{
    <COMPACT> { pos = getPos(); }
    <MATERIALIZATION>
    { materializationPath = CompoundIdentifier(); }
    <AS>
    { newMaterializationId = StringLiteral(); }
    {
        return new SqlCompactMaterialization(pos, materializationPath, newMaterializationId);
    }
}

/**
 * Parses a ANALYZE TABLE STATISTICS
 */
SqlNode SqlAnalyzeTableStatistics() :
{
    final SqlParserPos pos;
    final SqlIdentifier table;
    SqlLiteral isAnalyze = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    SqlNodeList columns = SqlNodeList.EMPTY;
}
{
    <ANALYZE> { pos = getPos(); }
    <TABLE> { table = CompoundIdentifier(); }
    <FOR>
      (
        <ALL> <COLUMNS> { columns = SqlNodeList.EMPTY; }
        |
        <COLUMNS> { columns = ParseOptionalFieldList("Columns"); }
      )
    (
      <COMPUTE> <STATISTICS> { isAnalyze = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
      |
      <DELETE> <STATISTICS> { isAnalyze = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); }
    )
    { return new SqlAnalyzeTableStatistics(pos, table, isAnalyze, columns); }
}

/**
 * Parses a REFRESH DATASET table_name statement
 */
SqlNode SqlRefreshDataset() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlLiteral deleteUnavail = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral promotion = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral forceUp = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral allFilesRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral allPartitionsRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral fileRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral partitionRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlNodeList filesList = SqlNodeList.EMPTY;
    SqlNodeList partitionList = SqlNodeList.EMPTY;
}
{
    <REFRESH> { pos = getPos(); }
    <DATASET>
    { tblName = CompoundIdentifier(); }
    (
      <FOR> <ALL>
      (
        <FILES> { allFilesRefresh = SqlLiteral.createBoolean(true, pos); }
      |
        <PARTITIONS> { allPartitionsRefresh = SqlLiteral.createBoolean(true, pos); }
      )
    |
      <FOR> <FILES> {
        fileRefresh = SqlLiteral.createBoolean(true, pos);
        filesList = ParseRequiredFilesList();
      }
    |
      <FOR> <PARTITIONS> {
        partitionRefresh = SqlLiteral.createBoolean(true, pos);
        partitionList = ParseRequiredPartitionList();
      }
    )?
    (
      <AUTO> <PROMOTION> { promotion = SqlLiteral.createBoolean(true, pos); }
    |
      <AVOID> <PROMOTION> { promotion = SqlLiteral.createBoolean(false, pos); }
    )?
    (
      <FORCE> <UPDATE> { forceUp = SqlLiteral.createBoolean(true, pos); }
    |
      <LAZY> <UPDATE> { forceUp = SqlLiteral.createBoolean(false, pos); }
    )?
    (
      <DELETE> <WHEN> <MISSING> { deleteUnavail = SqlLiteral.createBoolean(true, pos); }
    |
      <MAINTAIN> <WHEN> <MISSING> { deleteUnavail = SqlLiteral.createBoolean(false, pos); }
    )?
    { return new SqlRefreshDataset(pos, tblName, deleteUnavail, forceUp, promotion, allFilesRefresh,
        allPartitionsRefresh, fileRefresh, partitionRefresh, filesList, partitionList); }
}

/**
 * Parse a nullable option, default is true.
 */
 boolean NullableOptDefaultTrue() :
 {
 }
 {
     <NULL> { return true; }
 |
     <NOT> <NULL> { return false; }
 |
     { return true; }
 }


/**
 * Parse a "name1  type1 [NULL | NOT NULL], name2 type2 [NULL | NOT NULL] ..." list,
 * the field type default is nullable.
 */
 void FieldNameStructTypeCommaList(
         List<SqlIdentifier> fieldNames,
         List<SqlComplexDataTypeSpec> fieldTypes) :
 {
     SqlIdentifier fName;
     SqlDataTypeSpec fType;
     boolean nullable;
 }
 {
     fName = SimpleIdentifier()
     (<COLON>)?
     fType = DataType()
     nullable = NullableOptDefaultTrue()
     {
         fieldNames.add(fName);
         fieldTypes.add(new SqlComplexDataTypeSpec(fType.withNullable(nullable)));
     }
     (
         <COMMA>
         fName = SimpleIdentifier()
         (<COLON>)?
         fType = DataType()
         nullable = NullableOptDefaultTrue()
         {
             fieldNames.add(fName);
             fieldTypes.add(new SqlComplexDataTypeSpec(fType.withNullable(nullable)));
         }
     )*
 }

 /**
 * Parse Row type with format: ROW(name1  type1, name2 type2).
 * Parse Row type with format: STRUCT<name1 : type1, name2 : type2>.
 * Every item type can have suffix of `NULL` or `NOT NULL` to indicate if this type is nullable.
 * i.e. ROW(name1  type1 not null, name2 type2 null).
 */
 SqlIdentifier RowTypeName() :
 {
     List<SqlIdentifier> fieldNames = new ArrayList<SqlIdentifier>();
     List<SqlComplexDataTypeSpec> fieldTypes = new ArrayList<SqlComplexDataTypeSpec>();
 }
 {
     (<ROW> | <STRUCT>)
     (<LPAREN> | <LT>) FieldNameStructTypeCommaList(fieldNames, fieldTypes) (<RPAREN> | <GT>)
     {
         return new SqlRowTypeSpec(getPos(), fieldNames, fieldTypes);
     }
 }

 /**
  * Parse Array type with format: ARRAY(data_type).
  * Parse Array type with format: LIST<data_type>.
  * Every item type can have suffix of `NULL` or `NOT NULL` to indicate if this type is nullable.
  * i.e. col1 ARRAY(varchar NOT NULL).
  */
  SqlIdentifier ArrayTypeName() :
  {
      SqlDataTypeSpec fType;
      boolean nullable;
  }
  {
      (<ARRAY> | <LIST>)
      (<LPAREN> | <LT>)
       fType = DataType()
       nullable = NullableOptDefaultTrue()
      (<RPAREN> | <GT>)
      {
          return new SqlArrayTypeSpec(getPos(), new SqlComplexDataTypeSpec(fType.withNullable(nullable)));
      }
  }
