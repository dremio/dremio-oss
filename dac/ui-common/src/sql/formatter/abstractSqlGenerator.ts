/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import type { CommonTokenStream, RuleContext, Token } from "antlr4ts";
import type { ParseTreeListener } from "antlr4ts/tree/ParseTreeListener";
import type { ParseTreeVisitor } from "antlr4ts/tree/ParseTreeVisitor";
import type { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { DremioLexer } from "../../../target/generated-sources/antlr/DremioLexer";
import { DremioParser } from "../../../target/generated-sources/antlr/DremioParser";
import type { DremioParserVisitor } from "../../../target/generated-sources/antlr/DremioParserVisitor";
import { AbstractSQLLexer } from "../../../target/generated-sources/antlr/AbstractSQLLexer";
import { findAncestor } from "../parser/utils/ruleUtils";

enum RuleType {
  Command,
  Identifier,
  Function,
  Other,
}

type WithoutVisitPrefix<T> = T extends `visit${infer P}` ? P : never;
export type RuleName = Uncapitalize<
  WithoutVisitPrefix<
    Exclude<keyof DremioParserVisitor<any>, keyof ParseTreeVisitor<any>>
  >
>;

// KEY COMPONENT!
// Generating abstract SQL that can be used to format a query is dependent on manual labeling
// of the type of each parser rule, so the tokens can be known to be part of a command, or not.
// When adding or removing parser rules (e.g. in parserImpls.ftl) you will need to update this
// mapping depending on the type of the parser rule. Look at corresponding tests in sqlformatter-spec.ts
// to understand how this plays out. After editing this, please add/update unit tests as relevant
// to ensure your new SQL syntax will be formatted consistent with existing patterns.
const RULE_TYPES: { [K in RuleName]: RuleType } = {
  // These contain tokens that act as commands
  // The sequential tokens directly consumed by these rules will be formatted as a single unit
  sqlQueryEof: RuleType.Command,
  sqlStmtList: RuleType.Command,
  sqlStmt: RuleType.Command,
  sqlStmtEof: RuleType.Command,
  sqlShowTables: RuleType.Command,
  sqlShowViews: RuleType.Command,
  sqlShowFiles: RuleType.Command,
  sqlShowSchemas: RuleType.Command,
  sqlDescribeTable: RuleType.Command,
  sqlUseSchema: RuleType.Command,
  sqlCreateOrReplace: RuleType.Command,
  sqlDropFunction: RuleType.Command,
  sqlDescribeFunction: RuleType.Command,
  sqlShowFunctions: RuleType.Command,
  sqlDropView: RuleType.Command,
  sqlCreateTable: RuleType.Command,
  sqlInsertTable: RuleType.Command,
  sqlDeleteFromTable: RuleType.Command,
  sqlUpdateTable: RuleType.Command,
  sqlMergeIntoTable: RuleType.Command,
  sqlDropTable: RuleType.Command,
  sqlTruncateTable: RuleType.Command,
  sqlRefreshReflection: RuleType.Command,
  sqlLoadMaterialization: RuleType.Command,
  sqlCompactMaterialization: RuleType.Command,
  sqlAnalyzeTableStatistics: RuleType.Command,
  sqlRefreshDataset: RuleType.Command,
  sqlAccel: RuleType.Command,
  sqlCreateAggReflection: RuleType.Command,
  sqlCreateRawReflection: RuleType.Command,
  sqlAddExternalReflection: RuleType.Command,
  sqlAlterDatasetReflectionRouting: RuleType.Command,
  sqlAlterClearPlanCache: RuleType.Command,
  sqlExplainJson: RuleType.Command,
  sqlGrant: RuleType.Command,
  sqlGrantPrivilege: RuleType.Command,
  parseGranteeType: RuleType.Command,
  sqlRevoke: RuleType.Command,
  sqlGrantOwnership: RuleType.Command,
  sqlGrantRole: RuleType.Command,
  sqlCreateRole: RuleType.Command,
  sqlRevokeRole: RuleType.Command,
  sqlDropRole: RuleType.Command,
  sqlCreateUser: RuleType.Command,
  sqlDropUser: RuleType.Command,
  sqlAlterUser: RuleType.Command,
  sqlUseVersion: RuleType.Command,
  sqlShowBranches: RuleType.Command,
  sqlShowTags: RuleType.Command,
  sqlShowLogs: RuleType.Command,
  sqlCreateBranch: RuleType.Command,
  sqlCreateTag: RuleType.Command,
  sqlDropBranch: RuleType.Command,
  sqlDropTag: RuleType.Command,
  sqlMergeBranch: RuleType.Command,
  sqlAssignBranch: RuleType.Command,
  sqlAssignTag: RuleType.Command,
  sqlSelect: RuleType.Command,
  sqlQueryOrDml: RuleType.Command,
  sqlDescribe: RuleType.Command,
  sqlProcedureCall: RuleType.Command,
  natural: RuleType.Command,
  joinType: RuleType.Command,
  joinTable: RuleType.Command,
  explicitTable: RuleType.Command,
  tableConstructor: RuleType.Command,
  whereOpt: RuleType.Command,
  groupByOpt: RuleType.Command,
  havingOpt: RuleType.Command,
  windowOpt: RuleType.Command,
  windowSpecification: RuleType.Command,
  orderBy: RuleType.Command,
  pivot: RuleType.Command,
  unpivot: RuleType.Command,
  matchRecognizeOpt: RuleType.Command,
  binaryQueryOperator: RuleType.Command,
  sqlExpressionEof: RuleType.Command,
  sqlSetOption: RuleType.Command,
  sqlAlter: RuleType.Command,
  sqlTypeName: RuleType.Command,
  withList: RuleType.Command,
  sqlRollbackTable: RuleType.Command,
  sqlVacuum: RuleType.Command,
  sqlOptimize: RuleType.Command,
  orderByLimitOpt: RuleType.Command,
  sqlCopyInto: RuleType.Command,
  dremioWhenMatchedClause: RuleType.Command,
  dremioWhenNotMatchedClause: RuleType.Command,
  explainDetailLevel: RuleType.Command,
  explainDepth: RuleType.Command,
  scope: RuleType.Command,
  sqlVacuumTable: RuleType.Command,
  sqlVacuumCatalog: RuleType.Command,
  sqlExplainQueryDML: RuleType.Command,
  sqlCreateFolder: RuleType.Command,
  sqlDropFolder: RuleType.Command,
  parseReferenceType: RuleType.Command,
  aTVersionSpec: RuleType.Command,
  aTBranchVersionOrReferenceSpec: RuleType.Command,
  qualifyOpt: RuleType.Command,
  sqlShowCreate: RuleType.Command,
  sqlShowTableProperties: RuleType.Command,
  sqlCreatePipe: RuleType.Command,
  sqlDescribePipe: RuleType.Command,
  sqlShowPipes: RuleType.Command,

  // These comprise identifier tokens
  // Note: Identifier list rules should not be added here
  stringLiteral: RuleType.Identifier,
  identifierSegment: RuleType.Identifier,
  identifier: RuleType.Identifier,
  simpleIdentifier: RuleType.Identifier,
  compoundIdentifier: RuleType.Identifier,
  nonReservedKeyWord: RuleType.Identifier,
  nonReservedKeyWord0of3: RuleType.Identifier,
  nonReservedKeyWord1of3: RuleType.Identifier,
  nonReservedKeyWord2of3: RuleType.Identifier,

  // These are usually named like "FunctionCall" and contain a function name, (and usually) left paren, args, right paren
  extendedBuiltinFunctionCall: RuleType.Function,
  builtinFunctionCall: RuleType.Function,
  timestampAddFunctionCall: RuleType.Function,
  timestampDiffFunctionCall: RuleType.Function,
  matchRecognizeFunctionCall: RuleType.Function,
  namedFunctionCall: RuleType.Function,
  namedRoutineCall: RuleType.Function,
  jdbcFunctionCall: RuleType.Function,
  parseFunctionFieldList: RuleType.Function,
  policy: RuleType.Function,
  policyWithoutArgs: RuleType.Function,
  luceneQuery: RuleType.Function,
  jsonExistsFunctionCall: RuleType.Function,
  jsonValueFunctionCall: RuleType.Function,
  jsonQueryFunctionCall: RuleType.Function,
  jsonObjectFunctionCall: RuleType.Function,
  jsonObjectAggFunctionCall: RuleType.Function,
  jsonArrayFunctionCall: RuleType.Function,
  jsonArrayAggFunctionCall: RuleType.Function,

  // Catch-all
  extendedTableRef: RuleType.Other,
  floorCeilOptions: RuleType.Other,
  orderedQueryOrExpr: RuleType.Other,
  leafQuery: RuleType.Other,
  parenthesizedExpression: RuleType.Other,
  parenthesizedQueryOrCommaList: RuleType.Other,
  parenthesizedQueryOrCommaListWithDefault: RuleType.Other,
  functionParameterList: RuleType.Other,
  arg0: RuleType.Other,
  arg: RuleType.Other,
  default: RuleType.Other,
  parseOptionalFieldList: RuleType.Other,
  parseOptionalFieldListWithMasking: RuleType.Other,
  parseRequiredFieldList: RuleType.Other,
  parseRequiredFieldListWithMasking: RuleType.Other,
  columnNamesWithMasking: RuleType.Other,
  functionKeyValuePair: RuleType.Other,
  fieldFunctionTypeCommaList: RuleType.Other,
  tableElementListWithMasking: RuleType.Other,
  tableElementList: RuleType.Other,
  tableElementWithMasking: RuleType.Other,
  tableElement: RuleType.Other,
  parsePartitionTransform: RuleType.Other,
  parsePartitionTransformList: RuleType.Other,
  nullableOptDefaultTrue: RuleType.Other,
  fieldNameStructTypeCommaList: RuleType.Other,
  rowTypeName: RuleType.Other,
  arrayTypeName: RuleType.Other,
  parseRequiredFilesList: RuleType.Other,
  stringLiteralCommaList: RuleType.Other,
  parseRequiredPartitionList: RuleType.Other,
  keyValueCommaList: RuleType.Other,
  keyValuePair: RuleType.Other,
  parseFieldListWithGranularity: RuleType.Other,
  simpleIdentifierCommaListWithGranularity: RuleType.Other,
  parseFieldListWithMeasures: RuleType.Other,
  simpleIdentifierCommaListWithMeasures: RuleType.Other,
  measureList: RuleType.Other,
  parseColumns: RuleType.Other,
  identifierCommaList: RuleType.Other,
  typedElement: RuleType.Other,
  conjunction: RuleType.Other,
  modifiers: RuleType.Other,
  query: RuleType.Other,
  clause: RuleType.Other,
  term: RuleType.Other,
  privilegeCommaList: RuleType.Other,
  privilege: RuleType.Other,
  tableWithVersionContext: RuleType.Other,
  whenMatchedClause: RuleType.Other,
  whenNotMatchedClause: RuleType.Other,
  selectList: RuleType.Other,
  selectItem: RuleType.Other,
  selectExpression: RuleType.Other,
  fromClause: RuleType.Other,
  tableRef: RuleType.Other,
  tableRef2: RuleType.Other,
  extendList: RuleType.Other,
  columnType: RuleType.Other,
  compoundIdentifierType: RuleType.Other,
  rowConstructorList: RuleType.Other,
  rowConstructor: RuleType.Other,
  groupingElementList: RuleType.Other,
  groupingElement: RuleType.Other,
  expressionCommaList: RuleType.Other,
  expressionCommaList2: RuleType.Other,
  windowRange: RuleType.Other,
  orderItem: RuleType.Other,
  pivotAgg: RuleType.Other,
  pivotValue: RuleType.Other,
  unpivotValue: RuleType.Other,
  measureColumnCommaList: RuleType.Other,
  measureColumn: RuleType.Other,
  patternExpression: RuleType.Other,
  patternTerm: RuleType.Other,
  patternFactor: RuleType.Other,
  patternPrimary: RuleType.Other,
  subsetDefinitionCommaList: RuleType.Other,
  subsetDefinition: RuleType.Other,
  patternDefinitionCommaList: RuleType.Other,
  patternDefinition: RuleType.Other,
  queryOrExpr: RuleType.Other,
  withItem: RuleType.Other,
  leafQueryOrExpr: RuleType.Other,
  expression: RuleType.Other,
  expression2b: RuleType.Other,
  expression2: RuleType.Other,
  comp: RuleType.Other,
  expression3: RuleType.Other,
  periodOperator: RuleType.Other,
  collateClause: RuleType.Other,
  unsignedNumericLiteralOrParam: RuleType.Other,
  atomicRowExpression: RuleType.Other,
  caseExpression: RuleType.Other,
  sequenceExpression: RuleType.Other,
  literal: RuleType.Other,
  unsignedNumericLiteral: RuleType.Other,
  numericLiteral: RuleType.Other,
  specialLiteral: RuleType.Other,
  dateTimeLiteral: RuleType.Other,
  multisetConstructor: RuleType.Other,
  arrayConstructor: RuleType.Other,
  mapConstructor: RuleType.Other,
  periodConstructor: RuleType.Other,
  intervalLiteral: RuleType.Other,
  intervalQualifier: RuleType.Other,
  timeUnit: RuleType.Other,
  timestampInterval: RuleType.Other,
  dynamicParam: RuleType.Other,
  simpleIdentifierCommaList: RuleType.Other,
  parenthesizedSimpleIdentifierList: RuleType.Other,
  simpleIdentifierOrList: RuleType.Other,
  compoundIdentifierTypeCommaList: RuleType.Other,
  parenthesizedCompoundIdentifierList: RuleType.Other,
  newSpecification: RuleType.Other,
  unsignedIntLiteral: RuleType.Other,
  intLiteral: RuleType.Other,
  dataType: RuleType.Other,
  typeName: RuleType.Other,
  jdbcOdbcDataTypeName: RuleType.Other,
  jdbcOdbcDataType: RuleType.Other,
  collectionsTypeName: RuleType.Other,
  cursorExpression: RuleType.Other,
  matchRecognizeCallWithModifier: RuleType.Other,
  matchRecognizeNavigationLogical: RuleType.Other,
  matchRecognizeNavigationPhysical: RuleType.Other,
  standardFloorCeilOptions: RuleType.Other,
  nonReservedJdbcFunctionName: RuleType.Other,
  functionName: RuleType.Other,
  reservedFunctionName: RuleType.Other,
  contextVariable: RuleType.Other,
  binaryMultisetOperator: RuleType.Other,
  binaryRowOperator: RuleType.Other,
  prefixRowOperator: RuleType.Other,
  postfixRowOperator: RuleType.Other,
  unusedExtension: RuleType.Other,
  tableOverOpt: RuleType.Other,
  sqlSelectKeywords: RuleType.Other,
  sqlInsertKeywords: RuleType.Other,
  exprOrJoinOrOrderedQuery: RuleType.Other,
  parseFunctionReturnFieldList: RuleType.Other,
  functionReturnTypeCommaList: RuleType.Other,
  returnKeyValuePair: RuleType.Other,
  parseCopyIntoOptions: RuleType.Other,
  parseOptimizeOptions: RuleType.Other,
  dremioRowTypeName: RuleType.Other,
  sqlDropReflection: RuleType.Other,
  dQuery: RuleType.Other,
  join: RuleType.Other,
  tableRef1: RuleType.Other,
  tableRef3: RuleType.Other,
  tableFunctionCall: RuleType.Other, // Not actually a function but a subquery (usually)
  tablesample: RuleType.Other,
  addSetOpQuery: RuleType.Other,
  sqlTypeName1: RuleType.Other,
  sqlTypeName2: RuleType.Other,
  sqlTypeName3: RuleType.Other,
  nullableOptDefaultFalse: RuleType.Other,
  fieldNameTypeCommaList: RuleType.Other,
  characterTypeName: RuleType.Other,
  dateTimeTypeName: RuleType.Other,
  precisionOpt: RuleType.Other,
  timeZoneOpt: RuleType.Other,
  parseTableProperty: RuleType.Other,
  vacuumTableExpireSnapshotOptions: RuleType.Other,
  vacuumTableRemoveOrphanFilesOptions: RuleType.Other,
  sqlQueryOrTableDml: RuleType.Other,
  parenthesizedKeyValueOptionCommaList: RuleType.Other,
  keyValueOption: RuleType.Other,
  commaSepatatedSqlHints: RuleType.Other,
  tableRefWithHintsOpt: RuleType.Other,
  jsonRepresentation: RuleType.Other,
  jsonInputClause: RuleType.Other,
  jsonReturningClause: RuleType.Other,
  jsonOutputClause: RuleType.Other,
  jsonValueExpression: RuleType.Other,
  jsonPathSpec: RuleType.Other,
  jsonApiCommonSyntax: RuleType.Other,
  jsonExistsErrorBehavior: RuleType.Other,
  jsonValueEmptyOrErrorBehavior: RuleType.Other,
  jsonQueryEmptyOrErrorBehavior: RuleType.Other,
  jsonQueryWrapperBehavior: RuleType.Other,
  jsonName: RuleType.Other,
  jsonNameAndValue: RuleType.Other,
  jsonConstructorNullClause: RuleType.Other,
};

// These are tokens that are not considered part of commands (even when directly
// emitted by a command-labeled rule), and that get mapped to specific AbstractSQL
// tokens so they can play into formatting. E.g. LPAREN RPAREN to identify parenthesized
// blocks, COMMA to identify lists (each element newlined), AND OR to identify conjunctions
// (each newlined).
const SPECIAL_TOKENS: { [dremioToken: number]: AbstractSqlToken } = {
  [DremioLexer.LPAREN]: AbstractSQLLexer.LPAREN,
  [DremioLexer.RPAREN]: AbstractSQLLexer.RPAREN,
  [DremioLexer.COMMA]: AbstractSQLLexer.COMMA,
  [DremioLexer.SEMICOLON]: AbstractSQLLexer.SEMICOLON,
  [DremioLexer.AND]: AbstractSQLLexer.AND,
  [DremioLexer.OR]: AbstractSQLLexer.OR,
};

// Special tokens that by default cannot start a command, only continue a command
// e.g. continue: CREATE *OR* REPLACE VDS, CREATE TABLE tbl STORE *AS* (blah)
// e.g. no start: FROM tbl *AS* blah
const NON_COMMAND_START_WORDS = [
  DremioLexer.AND,
  DremioLexer.OR,
  DremioLexer.AS,
  DremioLexer.AT,
  DremioLexer.REF,
  DremioLexer.REFERENCE,
  DremioLexer.BRANCH,
  DremioLexer.TAG,
  DremioLexer.COMMIT,
];

// Overrides NON_COMMAND_START_WORDS on a per-rule basis
// i.e. allows for the token to start a command
// e.g. CREATE OR REPLACE VDS PARTITION BY (blah) *AS* blah2
const PER_RULE_COMMAND_START_WORDS: Partial<{ [K in RuleName]: number[] }> = {
  sqlCreateTable: [DremioLexer.AS],
  sqlCreateOrReplace: [DremioLexer.AS],
};

type CommandWordOverrides = {
  type: "only" | "exclude";
  tokens: number[];
};

// Defines tokens that should be considered the only command tokens in a command rule,
// or excluded from the command tokens in a command rule.
// Note this does not affect NON_COMMAND_START_WORDS, those will not be treated as start
// of the command unless overridden in PER_RULE_COMMAND_START_WORDS
//
// Be careful excluding/limiting command tokens as it may cause unexpected results if one
// precedes a non-parenthesized list.
// E.g. PIVOT (AVG(col) FOR col IN (value)) - if you add pivot() rule override for only "PIVOT"
// if the FOR parameter is really a list, you would end up with weird formatting like:
// PIVOT(AVG(col) FOR col1,               since list elements are formatted context-agnostic
//       col2 IN (value))
const PER_RULE_COMMAND_WORD_OVERRIDES: Partial<{
  [K in RuleName]: CommandWordOverrides;
}> = {
  orderByLimitOpt: {
    type: "only",
    tokens: [DremioLexer.LIMIT],
  },
  sqlVacuum: {
    type: "exclude",
    tokens: [DremioLexer.RETAIN_LAST],
  },
};

export type AbstractSqlToken =
  | typeof AbstractSQLLexer.COMMAND
  | typeof AbstractSQLLexer.NONCOMMAND
  | typeof AbstractSQLLexer.LPAREN
  | typeof AbstractSQLLexer.RPAREN
  | typeof AbstractSQLLexer.COMMA
  | typeof AbstractSQLLexer.SEMICOLON
  | typeof AbstractSQLLexer.AND
  | typeof AbstractSQLLexer.OR;

/**
 * Metadata about the DremioLexer token within the specific rule context that produced it
 */
export type OriginalTokenInfo = {
  text: string;
  inFunction: boolean;
  lineNum: number;
  hiddenTokensAfter: HiddenTokens;
  requiresLeadingSpace: boolean;
};

export type OriginalTokenInfoFirst = OriginalTokenInfo & {
  hiddenTokensBefore: HiddenTokens;
};

export enum HiddenTokenType {
  SINGLE_LINE_COMMENT,
  MULTI_LINE_COMMENT,
}

/**
 * These tokens are not emitted in AbstractSql (and were not terminal nodes of the Dremio ParseTree)
 * They are tracked separately so the formatted query can be rehydrated with them at the correct position.
 */
export type HiddenTokens = [
  text: string,
  lineNum: number,
  startCol: number,
  type: HiddenTokenType
][];

/* First element contains pre-hidden tokens, subsequent ones only contain post-hidden tokens */
export type SingleStatementOriginalTokensInfo = [
  OriginalTokenInfoFirst,
  ...OriginalTokenInfo[]
];

/* One list element per SQL statement */
export type OriginalTokensInfo = SingleStatementOriginalTokensInfo[];

/**
 * This class implements a listener of a Dremio-parsed ParseTree
 * The purpose is to visit all terminal nodes (i.e. that directly correspond to lexed tokens)
 * and convert each to an AbstractSql token, capable of being parsed by AbstractSqlParser
 * Additionally, we return contextual information about each new token, that is required in
 * order to correctly format it (see OriginalTokenInfo).
 */
export class AbstractSqlGenerator implements ParseTreeListener {
  private dremioSqlTokenStream: CommonTokenStream;
  private originalTokensInfo: OriginalTokensInfo = [];
  private abstractSqlTokens: AbstractSqlToken[] = [];
  private prevInCommand: boolean = false;
  private prevInFunction: boolean = false;
  private prevInIdentifier: boolean = false;
  private prevTerminal?: TerminalNode = undefined;
  private startNewStatement: boolean = true;

  constructor(dremioSqlTokenStream: CommonTokenStream) {
    this.dremioSqlTokenStream = dremioSqlTokenStream;
  }

  visitTerminal(node: TerminalNode) {
    if (node.symbol.type == -1) {
      return; // skip EOF
    }

    // Chain repeated semicolons as part of the previous statement
    if (this.startNewStatement && node._symbol.type !== DremioLexer.SEMICOLON) {
      // Avoid associating hidden tokens between statements with both stmt1 and stmt2: treat as pre-tokens for stmt2
      if (this.originalTokensInfo.length > 0) {
        const currentStatementOriginalTokensInfo =
          this.originalTokensInfo[this.originalTokensInfo.length - 1];
        const lastTerminalOriginalTokenInfo =
          currentStatementOriginalTokensInfo.pop()!;
        lastTerminalOriginalTokenInfo.hiddenTokensAfter = []; // They should only be "before" tokens of the next statement
        currentStatementOriginalTokensInfo.push(lastTerminalOriginalTokenInfo);
      }

      // We cast rather than simplify the type to OriginalTokenInfo[] so we can still give type safety to user of getAbstractSqlTokens
      this.originalTokensInfo.push(
        [] as unknown as SingleStatementOriginalTokensInfo
      );
      this.startNewStatement = false;
    }

    const [abstractSqlToken, originalTokenInfo] = this.generate(node);
    this.abstractSqlTokens.push(abstractSqlToken);

    const statementOriginalTokensInfo =
      this.originalTokensInfo[this.originalTokensInfo.length - 1];
    if (statementOriginalTokensInfo.length == 0) {
      statementOriginalTokensInfo.push({
        ...originalTokenInfo,
        hiddenTokensBefore: this.getPreHiddenTokens(node),
      });
    } else {
      statementOriginalTokensInfo.push(originalTokenInfo);
    }

    if (node._symbol.type === DremioLexer.SEMICOLON) {
      this.startNewStatement = true;
    }

    this.prevTerminal = node;
  }

  getAbstractSqlTokens(): [
    abstractSqlTokens: AbstractSqlToken[],
    originalTokensInfo: OriginalTokensInfo
  ] {
    return [this.abstractSqlTokens, this.originalTokensInfo];
  }

  private getPreHiddenTokens(
    node: TerminalNode
  ): [string, number, number, HiddenTokenType][] {
    const tokenIdx = node.symbol.tokenIndex;
    return this.dremioSqlTokenStream
      .getHiddenTokensToLeft(tokenIdx)
      .map((token) => [
        token.text || "",
        token.line,
        token.charPositionInLine,
        this.getHiddenTokenType(token),
      ]);
  }

  private getPostHiddenTokens(
    node: TerminalNode
  ): [string, number, number, HiddenTokenType][] {
    const tokenIdx = node.symbol.tokenIndex;
    return this.dremioSqlTokenStream
      .getHiddenTokensToRight(tokenIdx)
      .map((token) => [
        token.text || "",
        token.line,
        token.charPositionInLine,
        this.getHiddenTokenType(token),
      ]);
  }

  private getHiddenTokenType(token: Token): HiddenTokenType {
    // Can't validate if neither of these two because the token names are generated directly from the upstream
    // javacc grammar and it doesn't name the multi line comment tokens (so they have arbitrary names)
    return token.type == DremioLexer.SINGLE_LINE_COMMENT
      ? HiddenTokenType.SINGLE_LINE_COMMENT
      : HiddenTokenType.MULTI_LINE_COMMENT;
  }

  private generate(node: TerminalNode): [AbstractSqlToken, OriginalTokenInfo] {
    const inIdentifier = this.isIdentifierToken(node);
    const inFunction = this.isFunctionToken(node);
    const isCommandToken = this.isCommandToken(node) && !inFunction; // function is lower scope
    const abstractSqlSpecialTokenType =
      this.toAbstractSqlSpecialTokenType(node);
    let abstractSqlToken: AbstractSqlToken;

    if (inFunction) {
      // Highest precedence to mitigate potential rule mislabeling
      // Once in a function, all descendents considered part of the function, for formatting purposes
      abstractSqlToken = AbstractSQLLexer.NONCOMMAND;
    } else if (isCommandToken) {
      abstractSqlToken = AbstractSQLLexer.COMMAND;
    } else if (abstractSqlSpecialTokenType != undefined) {
      abstractSqlToken = abstractSqlSpecialTokenType;
    } else {
      abstractSqlToken = AbstractSQLLexer.NONCOMMAND;
    }

    const originalTokenInfo = this.createOriginalTokenInfo(
      node,
      inFunction,
      inIdentifier
    );
    this.prevInCommand = isCommandToken;
    this.prevInFunction = inFunction;
    this.prevInIdentifier = inIdentifier;
    return [abstractSqlToken, originalTokenInfo];
  }

  private createOriginalTokenInfo(
    terminal: TerminalNode,
    inFunction: boolean,
    inIdentifier: boolean
  ): OriginalTokenInfo {
    return {
      text: terminal.symbol.text || "",
      inFunction: !!inFunction,
      lineNum: terminal.symbol.line,
      hiddenTokensAfter: this.getPostHiddenTokens(terminal),
      requiresLeadingSpace: this.requiresLeadingSpace(
        terminal,
        inIdentifier,
        inFunction
      ),
    };
  }

  /* By default the formatter treats all tokens as requiring pre-space, so we need to tell it which do not */
  private requiresLeadingSpace(
    terminal: TerminalNode,
    inIdentifier: boolean,
    inFunction: boolean
  ): boolean {
    const type = terminal.symbol.type;
    const text = terminal.text;
    const prevType = this.prevTerminal?.symbol.type;
    // Space1.vds1 should not have spaces around the . (wrong: space1 . vds1)
    // However, two separate sequential identifiers should be separated with spaces
    const continuesIdentifier =
      this.prevInIdentifier &&
      inIdentifier &&
      this.prevTerminal != null &&
      this.getRootIdentifierContext(this.prevTerminal) != null &&
      this.getRootIdentifierContext(this.prevTerminal) ==
        this.getRootIdentifierContext(terminal);
    // Functions should not have leading/trailing space (wrong: avg ( 1, 2)) (right: avg(1, 2))
    const isFunctionParenOrComma =
      inFunction &&
      (type == DremioLexer.LPAREN ||
        type == DremioLexer.RPAREN ||
        type == DremioLexer.COMMA);
    const isFunctionFirstArg =
      inFunction && this.prevInFunction && this.prevTerminal?.text == "(";
    // Repeated semicolons should not have spaces in between
    // For lucene functions (elasticsearch) there are several special cases
    const isNonSeparatedSymbol =
      type == DremioLexer.COMMA ||
      type == DremioLexer.SEMICOLON ||
      // Lucene reference https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
      type == DremioLexer.L_COLON || // lucene :
      type == DremioLexer.RANGEIN_END || // lucene ]
      type == DremioLexer.RANGEEX_END || // lucene }
      type == DremioLexer.FUZZY_SLOP || // lucene ~
      type == DremioLexer.CARAT || // lucene ^
      type == DremioLexer.L_RPAREN || // lucene )
      prevType == DremioLexer.CARAT ||
      prevType == DremioLexer.RANGEIN_START || // lucene [
      prevType == DremioLexer.RANGEEX_START || // lucene {
      prevType == DremioLexer.L_PLUS || // lucene +
      prevType == DremioLexer.L_MINUS || // lucene -
      (prevType == DremioLexer.REGEXPTERM && // lucene regex
        type == DremioLexer.TERM && // lucene term
        text == "i") || // case insensitive regex flag
      (prevType == DremioLexer.L_COLON &&
        type == DremioLexer.TERM &&
        (text.startsWith(">") || text.startsWith("<"))) ||
      (prevType != DremioLexer.L_COLON && type == DremioLexer.L_LPAREN);
    return (
      !continuesIdentifier &&
      !isFunctionParenOrComma &&
      !isFunctionFirstArg &&
      !isNonSeparatedSymbol
    );
  }

  private getRootIdentifierContext(
    terminal: TerminalNode
  ): RuleContext | undefined {
    let context: RuleContext | undefined = terminal._parent?.ruleContext;
    while (context && this.hasAncestorOfType(context, RuleType.Identifier)) {
      context = context._parent?.ruleContext;
    }
    return context;
  }

  private toAbstractSqlSpecialTokenType(
    node: TerminalNode
  ): AbstractSqlToken | undefined {
    const dremioLexerTokenType = node.symbol.type;
    return SPECIAL_TOKENS[dremioLexerTokenType];
  }

  private isCommandToken(node: TerminalNode): boolean {
    const ruleName = this.getRuleName(node._parent!.ruleContext);
    const ruleCommandOverrides = PER_RULE_COMMAND_WORD_OVERRIDES[ruleName];
    return (
      this.isRuleOfType(ruleName, RuleType.Command) &&
      this.isWordLikeToken(node) &&
      (!ruleCommandOverrides ||
        (ruleCommandOverrides.type == "only" &&
          ruleCommandOverrides.tokens.includes(node.symbol.type)) ||
        (ruleCommandOverrides.type == "exclude" &&
          !ruleCommandOverrides.tokens.includes(node.symbol.type))) &&
      (!NON_COMMAND_START_WORDS.includes(node.symbol.type) ||
        this.prevInCommand ||
        !!PER_RULE_COMMAND_START_WORDS[ruleName]?.includes(node.symbol.type))
    );
  }

  private getRuleName(ruleContext: RuleContext): RuleName {
    const ruleIndex: number = ruleContext.ruleIndex;
    return DremioParser.ruleNames[ruleIndex] as RuleName;
  }

  private isRuleOfType(ruleName: RuleName, ruleType: RuleType): boolean {
    return RULE_TYPES[ruleName] == ruleType;
  }

  private isWordLikeToken(node: TerminalNode): boolean {
    return !!node.text.match(/^[a-zA-Z\-_]+$/);
  }

  // For commands we only want to consider them command tokens if directly emitted by a labeled rule
  // But for below is*Token functions we check for ancestor type rather than direct rule type as
  // tokens can be emitted in parser rules that may or may not correspond to
  // that semantic type (so we can't unambigously label them)

  private isFunctionToken(node: TerminalNode): boolean {
    return this.hasAncestorOfType(node, RuleType.Function);
  }

  private isIdentifierToken(node: TerminalNode): boolean {
    return this.hasAncestorOfType(node, RuleType.Identifier);
  }

  private hasAncestorOfType(
    context: TerminalNode | RuleContext,
    ruleType: RuleType
  ): boolean {
    const isType = (ruleContext: RuleContext) => {
      const ruleName = this.getRuleName(ruleContext);
      return this.isRuleOfType(ruleName, ruleType);
    };
    return !!findAncestor(context, isType);
  }
}
