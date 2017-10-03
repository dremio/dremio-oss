<#--

    Copyright (C) 2017 Dremio Corporation

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
 * Parses statement
 *   SHOW TABLES [{FROM | IN} db_name] [LIKE 'pattern']
 */
SqlNode SqlShowTables() :
{
    SqlParserPos pos;
    SqlIdentifier db = null;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <TABLES>
    [
        (<FROM> | <IN>) { db = CompoundIdentifier(); }
    ]
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowTables(pos, db, likePattern);
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

/**
 * Parses a create view or replace existing view statement.
 *   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateOrReplaceView() :
{
    SqlParserPos pos;
    boolean replaceView = false;
    SqlIdentifier viewName;
    SqlNode query;
    SqlNodeList fieldList;
}
{
    <CREATE> { pos = getPos(); }
    [ <OR> <REPLACE> { replaceView = true; } ]
    (<VIEW>|<VDS>)
    viewName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("View")
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(pos, viewName, fieldList, query, replaceView);
    }
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

/**
 * Parses a CTAS statement.
 * CREATE TABLE tblname [ (field1, field2, ...) ]
 *       [ (STRIPED, HASH, ROUNDROBIN) PARTITION BY (field1, field2, ..) ]
 *       [ DISTRIBUTE BY (field1, field2, ..) ]
 *       [ LOCALSORT BY (field1, field2, ..) ]
 *       [ STORE AS (opt1 => val1, opt2 => val3, ...) ]
 *       [ WITH SINGLE WRITER ]
 *       AS select_statement.
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    List<SqlNode> formatList = new ArrayList();
    SqlNodeList formatOptions;
    PartitionDistributionStrategy partitionDistributionStrategy;
    SqlNodeList partitionFieldList;
    SqlNodeList distributeFieldList;
    SqlNodeList sortFieldList;
    SqlLiteral singleWriter;
    SqlNode query;
}
{
    {
        partitionFieldList = SqlNodeList.EMPTY;
        distributeFieldList = SqlNodeList.EMPTY;
        sortFieldList =  SqlNodeList.EMPTY;
        formatOptions = SqlNodeList.EMPTY;
        singleWriter = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
    }
    <CREATE> { pos = getPos(); }
    <TABLE>
    tblName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("Table")
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
        partitionFieldList = ParseRequiredFieldList("Partition")
    )?
    (   <DISTRIBUTE> <BY>
        distributeFieldList = ParseRequiredFieldList("Distribution")
    )?
    (   <LOCALSORT> <BY>
        sortFieldList = ParseRequiredFieldList("Sort")
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
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateTable(pos, tblName, fieldList, partitionDistributionStrategy, partitionFieldList,
           formatOptions, singleWriter, query, sortFieldList, distributeFieldList);
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


