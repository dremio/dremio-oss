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
 * USE ( REF[ERENCE] | BRANCH | TAG | COMMIT ) <refValue>
 * [ AS OF timestamp ]
 * [ IN <sourceName> ]
 */
SqlNode SqlUseVersion() :
{
  SqlParserPos pos;
  ReferenceType refType;
  SqlIdentifier refValue;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
}
{
  <USE> { pos = getPos(); }
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
  [ <AS> <OF> timestamp = StringLiteral() ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlUseVersion(pos, refType, refValue, timestamp, sourceName); }
}

/**
 * SHOW BRANCHES [ IN sourceName ]
 */
SqlNode SqlShowBranches() :
{
  SqlParserPos pos;
  SqlIdentifier sourceName = null;
}
{
  <SHOW> { pos = getPos(); }
  <BRANCHES>
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlShowBranches(pos, sourceName);
  }
}

/**
 * SHOW TAGS [ IN sourceName ]
 */
SqlNode SqlShowTags() :
{
  SqlParserPos pos;
  SqlIdentifier sourceName = null;
}
{
  <SHOW> { pos = getPos(); }
  <TAGS>
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlShowTags(pos, sourceName);
  }
}

/**
 * SHOW LOG[S]
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ]
 * [ IN sourceName ]
 */
SqlNode SqlShowLogs() :
{
  SqlParserPos pos;
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
  SqlLiteral logExists = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);;
}
{
  <SHOW> { pos = getPos(); }
  (
    <LOG> { logExists = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
    |
    <LOGS>
  )
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
    [ <AS> <OF> timestamp = StringLiteral() ]
  ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlShowLogs(pos, refType, refValue, timestamp, sourceName, logExists); }
}

/**
 * CREATE BRANCH [ IF NOT EXISTS ] branchName
 * [ (FROM | AT) ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ]
 * [ IN sourceName ]
 */
SqlNode SqlCreateBranch() :
{
  SqlParserPos pos;
  SqlIdentifier branchName;
  SqlLiteral shouldErrorIfVersionExists = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
  PrepositionType prepositionType = null;
}
{
  <CREATE> { pos = getPos(); }
  <BRANCH>
  [ <IF> <NOT> <EXISTS> { shouldErrorIfVersionExists = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  branchName = SimpleIdentifier()
  [
    (
      <FROM> { prepositionType = PrepositionType.FROM; }
      |
      <AT> { prepositionType = PrepositionType.AT; }
    )
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
    [ <AS> <OF> timestamp = StringLiteral() ]
  ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlCreateBranch(pos, shouldErrorIfVersionExists, branchName, refType, refValue, timestamp, sourceName, prepositionType); }
}

/**
 * CREATE TAG [ IF NOT EXISTS ] tagName
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ]
 * [ IN sourceName ]
 */
SqlNode SqlCreateTag() :
{
  SqlParserPos pos;
  SqlIdentifier tagName;
  SqlLiteral shouldErrorIfVersionExists = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
  PrepositionType prepositionType = null;
}
{
  <CREATE> { pos = getPos(); }
  <TAG>
  [ <IF> <NOT> <EXISTS> { shouldErrorIfVersionExists = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  tagName = SimpleIdentifier()
  [
    (
      <FROM> { prepositionType = PrepositionType.FROM; }
      |
      <AT> { prepositionType = PrepositionType.AT; }
    )
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
    [ <AS> <OF> timestamp = StringLiteral() ]
  ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlCreateTag(pos, shouldErrorIfVersionExists, tagName, refType, refValue, timestamp, sourceName, prepositionType); }
}

/**
 * DROP BRANCH [ IF EXISTS ] branchName
 * [ AT COMMIT commitHash | FORCE ]
 * [ IN sourceName ]
 */
SqlNode SqlDropBranch() :
{
  SqlParserPos pos;
  SqlLiteral shouldErrorIfBranchDoesNotExist = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  SqlIdentifier branchName;
  SqlIdentifier commitHash = null;
  SqlLiteral forceDrop = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier sourceName = null;
}
{
  <DROP> { pos = getPos(); }
  <BRANCH>
  [ <IF> <EXISTS> { shouldErrorIfBranchDoesNotExist = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  branchName = SimpleIdentifier()
  [
    <AT> <COMMIT> { commitHash = SimpleIdentifier(); }
    |
    <FORCE> { forceDrop = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
  ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlDropBranch(pos, shouldErrorIfBranchDoesNotExist, branchName, commitHash, forceDrop, sourceName);
  }
}

/**
 * DROP TAG [ IF EXISTS ] tagName
 * [ AT COMMIT commitHash | FORCE ]
 * [ IN sourceName ]
 */
SqlNode SqlDropTag() :
{
  SqlParserPos pos;
  SqlLiteral shouldErrorIfTagDoesNotExist = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  SqlIdentifier tagName;
  SqlIdentifier commitHash = null;
  SqlLiteral forceDrop = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier sourceName = null;
}
{
  <DROP> { pos = getPos(); }
  <TAG>
  [ <IF> <EXISTS> { shouldErrorIfTagDoesNotExist = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  tagName = SimpleIdentifier()
  [
    <AT> <COMMIT> { commitHash = SimpleIdentifier(); }
    |
    <FORCE> { forceDrop = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
  ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlDropTag(pos, shouldErrorIfTagDoesNotExist, tagName, commitHash, forceDrop, sourceName);
  }
}

/**
 * MERGE BRANCH sourceBranchName
 * [INTO targetBranchName]
 * [IN <sourceName>]
 */
SqlNode SqlMergeBranch() :
{
  SqlParserPos pos;
  SqlIdentifier sourceBranchName;
  SqlIdentifier targetBranchName = null;
  SqlIdentifier sourceName = null;
}
{
  <MERGE> { pos = getPos(); }
  <BRANCH>
  sourceBranchName = SimpleIdentifier()
  [ <INTO> { targetBranchName = SimpleIdentifier(); } ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlMergeBranch(pos, sourceBranchName, targetBranchName, sourceName);
  }
}

/**
 * ALTER BRANCH branchName ASSIGN
 * ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue
 * [AS OF timestamp]
 * [ IN sourceName ]
 */
SqlNode SqlAssignBranch() :
{

  SqlParserPos pos;
  SqlIdentifier branchName;
  ReferenceType refType;
  SqlIdentifier refValue;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
}
{
  <ALTER> { pos = getPos(); }
  <BRANCH>
  branchName = SimpleIdentifier()
  <ASSIGN>
  (
    <REF> { refType = ReferenceType.REFERENCE; }
    |
    <REFERENCE> { refType = ReferenceType.REFERENCE; }
    |
    <BRANCH> { refType = ReferenceType.BRANCH; }
    |
    <TAG> { refType = ReferenceType.TAG; }
    |
    <COMMIT> {refType = ReferenceType.COMMIT; }
  )
  refValue = SimpleIdentifier()
  [ <AS> <OF> timestamp = StringLiteral() ]
  [<IN> {sourceName = SimpleIdentifier(); }]
  {
    return new SqlAssignBranch(pos, branchName, refType, refValue, timestamp, sourceName);
  }
}

/**
 * ALTER TAG tagName ASSIGN
 * ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue
 * [AS OF timestamp]
 * [ IN sourceName ]
 */
SqlNode SqlAssignTag() :
{
  SqlParserPos pos;
  SqlIdentifier tagName;
  ReferenceType refType;
  SqlIdentifier refValue;
  SqlNode timestamp = null;
  SqlIdentifier sourceName = null;
}
{
  <ALTER> { pos = getPos(); }
  <TAG>
  tagName = SimpleIdentifier()
  <ASSIGN>
  (
    <REF> { refType = ReferenceType.REFERENCE; }
    |
    <REFERENCE> { refType = ReferenceType.REFERENCE; }
    |
    <BRANCH> { refType = ReferenceType.BRANCH; }
    |
    <TAG> { refType = ReferenceType.TAG; }
    |
    <COMMIT> {refType = ReferenceType.COMMIT; }
  )
  refValue = SimpleIdentifier()
  [ <AS> <OF> timestamp = StringLiteral() ]
  [<IN> { sourceName = SimpleIdentifier();}]
  {
    return new SqlAssignTag(pos, tagName, refType, refValue, timestamp, sourceName);
  }
}

/**
 * Table version specification - can occur after either a table identifier or a TABLE() function call.
 *
 * AT [SNAPSHOT|BRANCH|TAG|COMMIT|REF] version-specifier
 * [ AS OF timestamp ]
 */
SqlNode TableWithVersionContext(SqlNode tableRef) :
{
    SqlParserPos pos;
    TableVersionType type;
    SqlIdentifier simpleId;
    SqlIdentifier tableId;
    SqlNode specifier;
    SqlCall call;
    SqlUnresolvedVersionedTableMacro tableMacro;
    SqlBasicCall collectionTableCall;
    SqlBasicCall functionCall;
    List<SqlNode> operands = new ArrayList<SqlNode>();
    List<String> timeTravelFunctionName = TableMacroNames.TIME_TRAVEL;
    SqlNode timestamp = null;
}
{
    <AT> { pos = getPos(); }
    (
        <SNAPSHOT> specifier = StringLiteral() { type = TableVersionType.SNAPSHOT_ID; }
    |
        <BRANCH> simpleId = SimpleIdentifier()
        {
            type = TableVersionType.BRANCH;
            specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
        }
    |
        <TAG> simpleId = SimpleIdentifier()
        {
            type = TableVersionType.TAG;
            specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
        }
    |
        <COMMIT> simpleId = SimpleIdentifier()
        {
            type = TableVersionType.COMMIT;
            specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
        }
    |
        (<REF> | <REFERENCE>) simpleId = SimpleIdentifier()
        {
            type = TableVersionType.REFERENCE;
            specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
        }
    |
        specifier = Expression(ExprContext.ACCEPT_NON_QUERY) { type = TableVersionType.TIMESTAMP; }
    )
    [ <AS> <OF> timestamp = StringLiteral() ]
    {
        if (tableRef.getKind() == SqlKind.IDENTIFIER) {
            // for SqlIdentifier table refs, we want to convert to calling our internal time travel
            // VersionedTableMacro implementation.  This is expected to be a macro that takes one argument
            // which is the table identifier converted to a string.  The function call itself must be
            // wrapped in a SqlVersionedTableMacroCall as this is the vehicle for passing along version info.
            tableId = (SqlIdentifier) tableRef;
            operands.add(SqlLiteral.createCharString(ParserUtil.unparseIdentifier(tableId), tableId.getParserPosition()));
            tableMacro = new SqlUnresolvedVersionedTableMacro(new SqlIdentifier(timeTravelFunctionName, tableId.getParserPosition()), type, specifier, timestamp);
            call = tableMacro.createCall(null, tableId.getParserPosition(), operands.toArray(new SqlNode[0]));
            return new SqlVersionedTableCollectionCall(pos, (SqlVersionedTableMacroCall)call);
        } else if (tableRef.getKind() == SqlKind.COLLECTION_TABLE) {
            // for the case where our tableRef is a TABLE(function()) call, we want to rewrite the call
            // with our SqlVersionedTableMacroCall wrapping the inner function call.
            collectionTableCall = (SqlBasicCall) tableRef;
            functionCall = collectionTableCall.operand(0);
            tableMacro = new SqlUnresolvedVersionedTableMacro(
                ((SqlFunction) functionCall.getOperator()).getSqlIdentifier(), type, specifier, timestamp);
            SqlVersionedTableMacroCall versionTableMacroCall = (SqlVersionedTableMacroCall) tableMacro.createCall(null, functionCall.getParserPosition(),
                functionCall.getOperands());
            return new SqlVersionedTableCollectionCall(pos, versionTableMacroCall);
        } else {
            throw generateParseException();
        }
    }
}
/**
 [AT (BRANCH | TAG | COMMIT | SNAPSHOT | TIMESTAMP (versionSpec)]
*/
SqlTableVersionSpec ATVersionSpec() :
{
    SqlParserPos pos;
    SqlIdentifier simpleId;
    TableVersionType tableVersionType = TableVersionType.NOT_SPECIFIED;
    SqlNode specifier = SqlLiteral.createCharString("NOT_SPECIFIED",SqlParserPos.ZERO);
}
{
    <AT> { pos = getPos(); }
     (
     <SNAPSHOT> specifier = StringLiteral() { tableVersionType = TableVersionType.SNAPSHOT_ID; }
     |
     <BRANCH> simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.BRANCH;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
     }
     |
     <TAG> simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.TAG;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
      }
     |
     <COMMIT> simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.COMMIT;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
      }
     |
     (<REF> | <REFERENCE>) simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.REFERENCE;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
      }
     |
      specifier = Expression(ExprContext.ACCEPT_NON_QUERY) { tableVersionType = TableVersionType.TIMESTAMP; }
     )
     {
       return new SqlTableVersionSpec(pos, tableVersionType, specifier);
     }
}
/**
 [AT BRANCH | REF | REFERENCE (versionSpec)]
*/
SqlTableVersionSpec ATBranchVersionOrReferenceSpec() :
{
    SqlParserPos pos;
    SqlIdentifier simpleId;
    TableVersionType tableVersionType = TableVersionType.NOT_SPECIFIED;
    SqlNode specifier = SqlLiteral.createCharString("NOT_SPECIFIED",SqlParserPos.ZERO);
}
{
    <AT> { pos = getPos(); }
     (
     <BRANCH> simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.BRANCH;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
     }
     |
     (<REF> | <REFERENCE>) simpleId = SimpleIdentifier()
     {
      tableVersionType = TableVersionType.REFERENCE;
      specifier = SqlLiteral.createCharString(simpleId.toString(), simpleId.getParserPosition());
      }
     )
     {
       return new SqlTableVersionSpec(pos, tableVersionType, specifier);
     }
}
