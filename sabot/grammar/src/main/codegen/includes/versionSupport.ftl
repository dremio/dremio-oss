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
 * MERGE BRANCH [ DRY RUN ] sourceBranchName
 * [ INTO targetBranchName]
 * [ IN <sourceName>]
 * [ ON CONFLICT ( OVERWRITE | DISCARD | CANCEL )
 *  [ EXCEPT ( OVERWRITE | DISCARD | CANCEL ) <content_name> [, <content_name>, ...]
 *    [ EXCEPT ( OVERWRITE | DISCARD | CANCEL ) <content_name> [, <content_name>, ...]]
 *  ]
 * ]
 */
SqlNode SqlMergeBranch() :
{
  SqlParserPos pos;
  SqlLiteral isDryRun = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  SqlIdentifier sourceBranchName;
  SqlIdentifier targetBranchName = null;
  SqlIdentifier sourceName = null;
  MergeBehavior defaultMergeBehavior = null;
  MergeBehavior mergeBehavior1 = null;
  MergeBehavior mergeBehavior2 = null;
  SqlNodeList exceptList1 = null;
  SqlNodeList exceptList2 = null;
}
{
  <MERGE> { pos = getPos(); }
  <BRANCH>
  [ <DRY> <RUN> { isDryRun = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); } ]
  sourceBranchName = SimpleIdentifier()
  [ <INTO> { targetBranchName = SimpleIdentifier(); } ]
  [ <IN> { sourceName = SimpleIdentifier(); } ]
  [ <ON> <CONFLICT>{ defaultMergeBehavior = getMergeBehavior(); }
    [<EXCEPT> { mergeBehavior1 = getMergeBehavior(); }
      exceptList1 = GetTableKeys()
    ]
    [<EXCEPT> { mergeBehavior2 = getMergeBehavior(); }
      exceptList2 = GetTableKeys()
    ]
  ]
  {
    return new SqlMergeBranch(pos, isDryRun, sourceBranchName, targetBranchName, sourceName, defaultMergeBehavior, mergeBehavior1, mergeBehavior2, exceptList1, exceptList2);
  }
}

MergeBehavior getMergeBehavior() :
{
  MergeBehavior mergeBehavior;
}
{
  (
    <OVERWRITE> { mergeBehavior = MergeBehavior.FORCE; }
    |
    <DISCARD> { mergeBehavior = MergeBehavior.DROP; }
    |
    <CANCEL> { mergeBehavior = MergeBehavior.NORMAL; }
  ) {
    return mergeBehavior;
  }
}

SqlNodeList GetTableKeys() :
{
  final Span s = Span.of();
  final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    TableKey(list)
    (
        <COMMA> TableKey(list)
    )* {
    return new SqlNodeList(list, s.end(this));
  }
}

void TableKey(List<SqlNode> list) :
{
  final Span s = Span.of();
  SqlIdentifier id;
}
{
  id = CompoundIdentifier() {
    list.add(id);
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
 * AT [SNAPSHOT|BRANCH|TAG|COMMIT|REF[ERENCE]|TIMESTAMP] version-specifier
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
    SqlTableVersionSpec sqlTableVersionSpec;
}
{
    sqlTableVersionSpec = ATVersionSpec()
    {
        pos = sqlTableVersionSpec.getPos();
        if (tableRef.getKind() == SqlKind.IDENTIFIER) {
            // for SqlIdentifier table refs, we want to convert to calling our internal time travel
            // VersionedTableMacro implementation.  This is expected to be a macro that takes one argument
            // which is the table identifier converted to a string.  The function call itself must be
            // wrapped in a SqlVersionedTableMacroCall as this is the vehicle for passing along version info.
            tableId = (SqlIdentifier) tableRef;
            operands.add(SqlLiteral.createCharString(ParserUtil.unparseIdentifier(tableId), tableId.getParserPosition()));
            tableMacro = new SqlUnresolvedVersionedTableMacro(new SqlIdentifier(timeTravelFunctionName, tableId.getParserPosition()), sqlTableVersionSpec);
            call = tableMacro.createCall(null, tableId.getParserPosition(), operands.toArray(new SqlNode[0]));
            return new SqlVersionedTableCollectionCall(pos, (SqlVersionedTableMacroCall)call);
        } else if (tableRef.getKind() == SqlKind.COLLECTION_TABLE) {
            // for the case where our tableRef is a TABLE(function()) call, we want to rewrite the call
            // with our SqlVersionedTableMacroCall wrapping the inner function call.
            collectionTableCall = (SqlBasicCall) tableRef;
            functionCall = collectionTableCall.operand(0);
            tableMacro = new SqlUnresolvedVersionedTableMacro(
                ((SqlFunction) functionCall.getOperator()).getSqlIdentifier(), sqlTableVersionSpec);
            SqlVersionedTableMacroCall versionTableMacroCall = (SqlVersionedTableMacroCall) tableMacro.createCall(null, functionCall.getParserPosition(),
                functionCall.getOperands());
            return new SqlVersionedTableCollectionCall(pos, versionTableMacroCall);
        } else {
            throw generateParseException();
        }
    }
}

/**
 * UDF version specification - can occur after a function call.
 *
 * AT [BRANCH|TAG|COMMIT|REF[ERENCE]|TIMESTAMP] version-specifier
 * [ AS OF timestamp ]
 */
SqlCall FunctionWithVersionContext(SqlCall call) :
{
    SqlParserPos pos;
    SqlTableVersionSpec sqlTableVersionSpec = null;
}
{
    sqlTableVersionSpec = ATVersionSpecWithoutTimeTravel()
    {
        pos = sqlTableVersionSpec.getPos();
        if (call instanceof SqlBasicCall &&
            sqlTableVersionSpec != null &&
            sqlTableVersionSpec.getTableVersionSpec().getTableVersionType() != TableVersionType.NOT_SPECIFIED) {
          return new SqlVersionedFunctionCall(call, sqlTableVersionSpec);
        }
        return call;
    }
}

/**
 * Table Function UDF version specification - can occur after a function call.
 *
 * AT [BRANCH|TAG|COMMIT|REF[ERENCE]|TIMESTAMP] version-specifier
 * [ AS OF timestamp ]
 */
SqlNode TableFunctionWithVersionContext(SqlNode call) :
{
    SqlParserPos pos;
    SqlTableVersionSpec sqlTableVersionSpec = null;
}
{
    sqlTableVersionSpec = ATVersionSpecWithoutTimeTravel()
    {
        pos = sqlTableVersionSpec.getPos();
        if (call instanceof SqlBasicCall &&
            sqlTableVersionSpec != null &&
            sqlTableVersionSpec.getTableVersionSpec().getTableVersionType() != TableVersionType.NOT_SPECIFIED) {
          return new SqlVersionedFunctionCall((SqlCall)call, sqlTableVersionSpec);
        }
        return call;
    }
}

/**
 [AT (BRANCH | REF | REFERENCE | TAG | COMMIT | SNAPSHOT | TIMESTAMP (versionSpec)]
 [ <AS> <OF> timestamp ]
 Note that the ATVersionSpec is meant to be used to specify a specific version of table. The specified version is not meant to be limited to be any specific type.
 If you mean to limit the version specification to the tip of a branch( or reference), use WriteableAtVersionSpec.
*/
SqlTableVersionSpec ATVersionSpec() :
{
    SqlParserPos pos;
    SqlIdentifier simpleId;
    TableVersionType tableVersionType = TableVersionType.NOT_SPECIFIED;
    SqlNode specifier = SqlLiteral.createCharString("NOT_SPECIFIED",SqlParserPos.ZERO);
    SqlNode timestamp = null;
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
     [ <AS> <OF> timestamp = StringLiteral() ]
     {
       return new SqlTableVersionSpec(pos, tableVersionType, specifier, timestamp);
     }
}

/**
 * [AT BRANCH | REF | REFERENCE (versionSpec)]
 * Note that REF and REFERENCE version types are not guaranteed to be writeable. They could point to
 * a tag or a commit as well. Since they might be a branch, we allow them in the grammar and check
 * them later.
 */
SqlTableVersionSpec WriteableAtVersionSpec() :
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
       return new SqlTableVersionSpec(pos, tableVersionType, specifier, null);
     }
}

/**
 [AT (BRANCH | REF | REFERENCE | TAG | COMMIT]
 [ <AS> <OF> timestamp ]
  Similar to AtVersionSpec() but not include At Snapshot and At Time stamp.
  This is a workaround for Sql parsers such as ShowFunctions.
*/
SqlTableVersionSpec ATVersionSpecWithoutTimeTravel() :
{
    SqlParserPos pos;
    SqlIdentifier simpleId;
    TableVersionType tableVersionType = TableVersionType.NOT_SPECIFIED;
    SqlNode specifier = SqlLiteral.createCharString("NOT_SPECIFIED",SqlParserPos.ZERO);
    SqlNode timestamp = null;
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
     )
     [ <AS> <OF> timestamp = StringLiteral() ]
     {
       return new SqlTableVersionSpec(pos, tableVersionType, specifier, timestamp);
     }
}
