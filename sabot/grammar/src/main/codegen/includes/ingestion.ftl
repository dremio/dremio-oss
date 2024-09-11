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

/**
 * COPY INTO <source>.<path>.<table_name>
 * [( <col_name> [, <col_name>... ])]
 *      FROM
        [location_clause |
        (SELECT [$<file_col_num> | <file_col_name>], [$<file_col_num> | <file_col_name> ]...
                FROM location_clause)]
 *       { FILES ( '<file_name>' [ , ... ] ) |
 *         REGEX '<regex_pattern>' }
 *
 * [ FILE_FORMAT { PARQUET | ORC | CSV | JSON | AVRO | XML } ]
 * [ ( format_option_clause [, ...]
 *     copy_option_clause [, ...] ) ]
 */
SqlNode SqlCopyInto() :
{
    SqlParserPos pos;
    SqlNode tableWithVersionContext = null;
    SqlNode location;
    List<SqlNode> fileList = new ArrayList<SqlNode>();
    SqlNodeList files = SqlNodeList.EMPTY;
    SqlNode file;
    SqlNode regexPattern = null;
    SqlNode fileFormat = null;
    SqlNodeList optionsList = SqlNodeList.EMPTY;
    SqlNodeList optionsValueList = SqlNodeList.EMPTY;
    List<SqlNode> selectMappingList = new ArrayList<SqlNode>();
    SqlNodeList mappings = SqlNodeList.EMPTY;
    SqlNode mapping;
    SqlNode select = null;
}
{
    <COPY> { pos = getPos(); }
    <INTO>
    tableWithVersionContext = CompoundIdentifier()
    [ tableWithVersionContext = TableWithVersionContext(tableWithVersionContext) ]
    (
      <LPAREN>
        mapping = SelectItem() {
            selectMappingList.add((SqlNode) mapping);
        }
        (
            <COMMA>
            mapping = SelectItem() {
              selectMappingList.add((SqlNode) mapping);
            }
        )*
        {
            mappings = new SqlNodeList(selectMappingList, getPos());
        }
      <RPAREN>
    )?
    <FROM>
    (
      (
        location = StringLiteral()
      )
      |
      (
        <LPAREN>
        select = QueryOrExpr(ExprContext.ACCEPT_QUERY)
        <FROM>
        location = StringLiteral()
        <RPAREN>
      )
    )
    (
          <FILES>
          <LPAREN>
              file = Literal() { fileList.add((SqlLiteral) file); }
          (
              <COMMA>
              file = Literal() {
                  fileList.add((SqlLiteral) file);
              }
          )*
          {
              files = new SqlNodeList(fileList, getPos());
          }
          <RPAREN>
          |
          <REGEX> {
          regexPattern = StringLiteral();
          }
    )?
    (
        <FILE_FORMAT>
        (
                fileFormat = Literal()
        )
    )?
    (
        <LPAREN>
          {
            optionsList = new SqlNodeList(getPos());
            optionsValueList = new SqlNodeList(getPos());
          }
            ParseCopyIntoOptions(optionsList, optionsValueList)
            (
                <COMMA>
                ParseCopyIntoOptions(optionsList, optionsValueList)
            )*
        <RPAREN>
    )?

    {
      return new SqlCopyIntoTable(pos, tableWithVersionContext, mappings, select, location, files, regexPattern, fileFormat, optionsList, optionsValueList);
    }
}

/**
 * Parse options for COPY INTO command.
 */
 void ParseCopyIntoOptions(SqlNodeList optionsList, SqlNodeList optionsValueList) :
 {
  SqlNode exp;
  SqlNodeList expList = SqlNodeList.EMPTY;
 }
 {
    (
        (
          // CSV and JSON
          <DATE_FORMAT> { optionsList.add(SqlLiteral.createCharString("DATE_FORMAT", getPos())); }
          |
          <TIME_FORMAT> { optionsList.add(SqlLiteral.createCharString("TIME_FORMAT", getPos())); }
          |
          <TIMESTAMP_FORMAT> { optionsList.add(SqlLiteral.createCharString("TIMESTAMP_FORMAT", getPos())); }
          |
          <TRIM_SPACE> { optionsList.add(SqlLiteral.createCharString("TRIM_SPACE", getPos())); }
          |
          // CSV specific
          <RECORD_DELIMITER> { optionsList.add(SqlLiteral.createCharString("RECORD_DELIMITER", getPos())); }
          |
          <FIELD_DELIMITER> { optionsList.add(SqlLiteral.createCharString("FIELD_DELIMITER", getPos())); }
          |
          <QUOTE_CHAR> { optionsList.add(SqlLiteral.createCharString("QUOTE_CHAR", getPos())); }
          |
          <ESCAPE_CHAR> { optionsList.add(SqlLiteral.createCharString("ESCAPE_CHAR", getPos())); }
          |
          <EMPTY_AS_NULL> { optionsList.add(SqlLiteral.createCharString("EMPTY_AS_NULL", getPos())); }
          |
          <ON_ERROR> { optionsList.add(SqlLiteral.createCharString("ON_ERROR", getPos())); }
          |
          <EXTRACT_HEADER> { optionsList.add(SqlLiteral.createCharString("EXTRACT_HEADER", getPos())); }
          |
          <SKIP_LINES> { optionsList.add(SqlLiteral.createCharString("SKIP_LINES", getPos())); }
        )
        exp = Literal() {
            optionsValueList.add(exp);
        }
    )
    |
    (
        <NULL_IF> { optionsList.add(SqlLiteral.createCharString("NULL_IF", getPos())); }
        <LPAREN>
        {
            expList = new SqlNodeList(getPos());
        }
        exp = Literal() { expList.add(exp); }
        (
            <COMMA>
            exp = Literal() { expList.add(exp); }
        )*
        <RPAREN>
        { optionsValueList.add(expList); }
    )
 }

/**
 * CREATE  PIPE [ IF NOT EXISTS ] <pipe_name>
 * [DEDUPE_LOOKBACK_PERIOD = <no of days>]
 * [NOTIFICATION_PROVIDER = [AWS_SQS | AZURE_STORAGE_QUEUE]]
 * [NOTIFICATION_QUEUE_REFERENCE = [<sqs_queue_id> | <azure_storage_queue_uri>]]
 * AS
 * <COPY INTO query>
**/
SqlNode SqlCreatePipe():
{
    SqlParserPos pos;
    boolean ifNotExists = false;
    SqlIdentifier pipeName = null;
    SqlNumericLiteral dedupeLookbackPeriod = null;
    SqlIdentifier notificationProvider = null;
    SqlIdentifier notificationQueueRef = null;
    SqlNode copyIntoNode;
}
{
    <CREATE> { pos = getPos(); }
    <PIPE>
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    pipeName = SimpleIdentifier()

    [ <DEDUPE_LOOKBACK_PERIOD> dedupeLookbackPeriod = UnsignedNumericLiteral() ]
    [
      <NOTIFICATION_PROVIDER> notificationProvider = SimpleIdentifier()
      <NOTIFICATION_QUEUE_REFERENCE> notificationQueueRef = SimpleIdentifier()
    ]

    <AS>
    (
        copyIntoNode = SqlCopyInto()
    )

    { return new SqlCreatePipe(pos, pipeName, copyIntoNode, dedupeLookbackPeriod, notificationProvider, notificationQueueRef, ifNotExists); }
}

/**
 * ALTER PIPE <pipe_name>
 * [DEDUPE_LOOKBACK_PERIOD = <no of days>]
 * AS
 * <COPY INTO query>
**/
SqlNode SqlAlterPipe():
{
    SqlParserPos pos;
    SqlIdentifier pipeName = null;
    SqlNumericLiteral dedupeLookbackPeriod = null;
    SqlNode copyIntoNode;
    boolean pipeExecutionStatus;
}
{
    <ALTER> { pos = getPos(); }
    <PIPE>
    pipeName = SimpleIdentifier()
    (
      <SET> <PIPE_EXECUTION_RUNNING> <EQ>
      (
        <TRUE> { pipeExecutionStatus = true;}
        |
        <FALSE> { pipeExecutionStatus = false; }
      )

      { return new SqlAlterPipeStatus(pos, pipeName, pipeExecutionStatus); }
      |
      [
          <DEDUPE_LOOKBACK_PERIOD> dedupeLookbackPeriod = UnsignedNumericLiteral()
      ]

      <AS>
      (
          copyIntoNode = SqlCopyInto()
      )
    )

    { return new SqlAlterPipe(pos, pipeName, copyIntoNode, dedupeLookbackPeriod); }
}

/**
 * DROP PIPE <pipe_name>
**/
SqlNode SqlDropPipe():
{
    SqlParserPos pos;
    SqlIdentifier pipeName = null;
}
{
    <DROP> { pos = getPos(); }
    <PIPE>
    pipeName = SimpleIdentifier()

    { return new SqlDropPipe(pos, pipeName); }
}

SqlNode SqlDescribePipe():
{
    SqlParserPos pos;
    SqlIdentifier pipeName = null;
}
{
    (<DESCRIBE> | <DESC>) { pos = getPos(); }
    <PIPE>
    {
        pipeName = SimpleIdentifier();
        return new SqlDescribePipe(pos, pipeName);
    }
}

SqlNode SqlTriggerPipe():
{
    SqlParserPos pos;
    SqlIdentifier pipeName = null;
    SqlIdentifier batchId = null;
}
{
    <TRIGGER> { pos = getPos(); }
    <PIPE>
    {
        pipeName = SimpleIdentifier();
    }
    [
      <FOR> <BATCH> { batchId = SimpleIdentifier(); }
    ]
    { return new SqlTriggerPipe(pos, pipeName, batchId); }
}
