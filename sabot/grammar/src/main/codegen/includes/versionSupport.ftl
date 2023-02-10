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
 * [ IN <sourceName> ]
 */
SqlNode SqlUseVersion() :
{
  SqlParserPos pos;
  ReferenceType refType;
  SqlIdentifier refValue;
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
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlUseVersion(pos, refType, refValue, sourceName); }
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
 * SHOW LOGS
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
 * [ IN sourceName ]
 */
SqlNode SqlShowLogs() :
{
  SqlParserPos pos;
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlIdentifier sourceName = null;
}
{
  <SHOW> { pos = getPos(); }
  <LOGS>
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
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlShowLogs(pos, refType, refValue, sourceName); }
}

/**
 * CREATE BRANCH [ IF NOT EXISTS ] branchName
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
 * [ IN sourceName ]
 */
SqlNode SqlCreateBranch() :
{
  SqlParserPos pos;
  SqlIdentifier branchName;
  SqlLiteral existenceCheck = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlIdentifier sourceName = null;
}
{
  <CREATE> { pos = getPos(); }
  <BRANCH>
  [ <IF> <NOT> <EXISTS> { existenceCheck = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  branchName = SimpleIdentifier()
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
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlCreateBranch(pos, existenceCheck, branchName, refType, refValue, sourceName); }
}

/**
 * CREATE TAG [ IF NOT EXISTS ] tagName
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
 * [ IN sourceName ]
 */
SqlNode SqlCreateTag() :
{
  SqlParserPos pos;
  SqlIdentifier tagName;
  SqlLiteral existenceCheck = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlIdentifier sourceName = null;
}
{
  <CREATE> { pos = getPos(); }
  <TAG>
  [ <IF> <NOT> <EXISTS> { existenceCheck = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); } ]
  tagName = SimpleIdentifier()
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
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  { return new SqlCreateTag(pos, existenceCheck, tagName, refType, refValue, sourceName); }
}

/**
 * DROP BRANCH [ IF EXISTS ] branchName
 * ( AT COMMIT commitHash | FORCE )
 * [ IN sourceName ]
 */
SqlNode SqlDropBranch() :
{
  SqlParserPos pos;
  SqlLiteral existenceCheck = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier branchName;
  SqlIdentifier commitHash = null;
  SqlLiteral forceDrop = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier sourceName = null;
}
{
  <DROP> { pos = getPos(); }
  <BRANCH>
  [ <IF> <EXISTS> { existenceCheck = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); } ]
  branchName = SimpleIdentifier()
  (
    <AT> <COMMIT> { commitHash = SimpleIdentifier(); }
    |
    <FORCE> { forceDrop = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
  )
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlDropBranch(pos, existenceCheck, branchName, commitHash, forceDrop, sourceName);
  }
}

/**
 * DROP TAG [ IF EXISTS ] tagName
 * ( AT COMMIT commitHash | FORCE )
 * [ IN sourceName ]
 */
SqlNode SqlDropTag() :
{
  SqlParserPos pos;
  SqlLiteral existenceCheck = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier tagName;
  SqlIdentifier commitHash = null;
  SqlLiteral forceDrop = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier sourceName = null;
}
{
  <DROP> { pos = getPos(); }
  <TAG>
  [ <IF> <EXISTS> { existenceCheck = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); } ]
  tagName = SimpleIdentifier()
  (
    <AT> <COMMIT> { commitHash = SimpleIdentifier(); }
    |
    <FORCE> { forceDrop = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
  )
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  {
    return new SqlDropTag(pos, existenceCheck, tagName, commitHash, forceDrop, sourceName);
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
 * [ IN sourceName ]
 */
SqlNode SqlAssignBranch() :
{

  SqlParserPos pos;
  SqlIdentifier branchName;
  ReferenceType refType;
  SqlIdentifier refValue;
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
  [<IN> {sourceName = SimpleIdentifier(); }]
  {
    return new SqlAssignBranch(pos, branchName, refType, refValue, sourceName);
  }
}

/**
 * ALTER TAG tagName ASSIGN
 * ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue
 * [ IN sourceName ]
 */
SqlNode SqlAssignTag() :
{
  SqlParserPos pos;
  SqlIdentifier tagName;
  ReferenceType refType;
  SqlIdentifier refValue;
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
  [<IN> { sourceName = SimpleIdentifier();}]
  {
    return new SqlAssignTag(pos, tagName, refType, refValue, sourceName);
  }
}

/**
 * Table version specification - can occur after either a table identifier or a TABLE() function call.
 *
 * AT [SNAPSHOT|BRANCH|TAG|COMMIT|REF] version-specifier
 */
SqlNode TableWithVersionContext(SqlNode tableRef) :
{
    SqlParserPos pos;
    TableVersionType type;
    SqlIdentifier simpleId;
    SqlIdentifier tableId;
    SqlNode specifier;
    SqlCall call;
    SqlBasicCall collectionTableCall;
    SqlBasicCall functionCall;
    List<SqlNode> list = Lists.newArrayList();
    List<String> timeTravelFunctionName = TableMacroNames.TIME_TRAVEL;
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
            type = TableVersionType.COMMIT_HASH_ONLY;
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
    {
        if (tableRef.getKind() == SqlKind.IDENTIFIER) {
            // for SqlIdentifier table refs, we want to convert to calling our internal time travel
            // VersionedTableMacro implementation.  This is expected to be a macro that takes one argument
            // which is the table identifier converted to a string.  The function call itself must be
            // wrapped in a SqlVersionedTableMacroCall as this is the vehicle for passing along version info.
            tableId = (SqlIdentifier) tableRef;
            list.add(SqlLiteral.createCharString(ParserUtil.unparseIdentifier(tableId), tableId.getParserPosition()));
            call = createCall(new SqlIdentifier(timeTravelFunctionName, tableId.getParserPosition()),
                tableId.getParserPosition(), SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, null, list);
            call = new SqlVersionedTableMacroCall(call.getOperator(), call.getOperandList().toArray(new SqlNode[0]),
                type, specifier, tableId.getComponent(tableId.names.size() - 1), tableId.getParserPosition());
            return new SqlVersionedTableCollectionCall(pos, (SqlVersionedTableMacroCall)call);
        } else if (tableRef.getKind() == SqlKind.COLLECTION_TABLE) {
            // for the case where our tableRef is a TABLE(function()) call, we want to rewrite the call
            // with our SqlVersionedTableMacroCall wrapping the inner function call.
            collectionTableCall = (SqlBasicCall) tableRef;
            functionCall = collectionTableCall.operand(0);
            SqlBasicCall versionTableMacroCall = new SqlVersionedTableMacroCall(functionCall.getOperator(),
                                                                                functionCall.getOperands(),
                                                                                type,
                                                                                specifier,
                                                                                null,
                                                                                functionCall.getParserPosition());
            return new SqlVersionedTableCollectionCall(pos, (SqlVersionedTableMacroCall)versionTableMacroCall);
        } else {
            throw generateParseException();
        }
    }
}
