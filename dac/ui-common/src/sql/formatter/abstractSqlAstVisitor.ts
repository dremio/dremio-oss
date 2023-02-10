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
import { RuleContext } from "antlr4ts";
import type { ErrorNode } from "antlr4ts/tree/ErrorNode";
import type { ParseTree } from "antlr4ts/tree/ParseTree";
import type { RuleNode } from "antlr4ts/tree/RuleNode";
import type { TerminalNode } from "antlr4ts/tree/TerminalNode";
import {
  StartContext,
  StatementSimpleContext,
  ParametersContext,
  ParenthesizedContext,
  ClausesContext,
  StatementParenthesizedContext,
  CommandContext,
  NoncommandContext,
  SeparatorContext,
} from "../../../dist-antlr/AbstractSQLParser";
import type { AbstractSQLParserVisitor } from "../../../dist-antlr/AbstractSQLParserVisitor";
import {
  type HiddenTokens,
  HiddenTokenType,
  type OriginalTokenInfo,
  type OriginalTokensInfo,
} from "./abstractSqlGenerator";

type VisitorResult = {
  type: "sql";
  sql: string[];
};

const SPACES_WITHIN_PAREN_BLOCK = 2;
const SPACES_AFTER_COMMAND = 1;

/**
 * AbstractSqlAstVisitor is responsible for implementing a visitor of an AbstractSql ParseTree
 * that serializes the tree as formatted SQL. It works by visiting each internal node, and recursively
 * building up formatted SQL lines. Parent rules are responsible for indenting their visited "children"
 * node results as necessary. In the case of terminal children, they must use the passed originalTokensInfo to
 * reconstruct the original token text, since the Abstract Sql is generalized to e.g. _CMD, _NONCMD:
 * the bare minimum needed to parse all possible SQL (a superset of the DremioParser rules). The abstract
 * SQL grammar is designed to parse the input in chunks that can be formatted in a definable way: e.g.
 * lists, parenthesized lists, expression conjunctions, and subqueries.
 *
 * This class should have no dependencies on DremioLexer/DremioParser
 */
export abstract class AbstractSqlAstVisitor
  implements AbstractSQLParserVisitor<VisitorResult>
{
  private originalTokensInfo: OriginalTokensInfo;

  constructor(originalTokensInfo: OriginalTokensInfo) {
    this.originalTokensInfo = originalTokensInfo;
  }

  /**
   * Join all statements, separated by a blank line. Each statement will be prefixed by any leading hidden tokens.
   */
  visitStart(ctx: StartContext): VisitorResult {
    const getSqlForStatement = (statementIdx: number): string => {
      const hiddenTokensLinesBefore = this.serializeHiddenTokens(
        this.originalTokensInfo[statementIdx][0].hiddenTokensBefore
      );
      const statementResult = statements[statementIdx].accept(this);
      const semicolons = ctx
        .SEMICOLON() // just semicolons after the statement before the next statement
        .filter((node: TerminalNode) => {
          const tokenIdx = node._symbol.tokenIndex;
          const isLastStatement = statementIdx == statements.length - 1;
          return (
            tokenIdx > statements[statementIdx].start.tokenIndex &&
            (isLastStatement ||
              tokenIdx < statements[statementIdx + 1].start.tokenIndex)
          );
        });
      const semicolonLines =
        semicolons.length > 0 ? this.serializeTerminals(semicolons) : [];
      const statementLines = this.mergeLinesWithOverlap(
        "",
        statementResult.sql,
        semicolonLines
      );
      return (
        [...hiddenTokensLinesBefore, ...statementLines]
          // single line comments may add blank lines, much less complex to just remove them at end
          .filter((line) => line.trim() != "")
          .join("\n")
      );
    };
    const statements = ctx.statement();
    let sql: string = getSqlForStatement(0);
    for (let i = 1; i < statements.length; i++) {
      sql += "\n\n";
      sql += getSqlForStatement(i);
    }
    return { type: "sql", sql: [sql] };
  }

  /**
   * Visit a non-parenthesized statement.
   */
  visitStatementSimple(ctx: StatementSimpleContext): VisitorResult {
    const sql = ctx.clauses().accept(this).sql;
    return { type: "sql", sql };
  }

  /**
   * Visit the child statement and surround in parentheses.
   */
  visitStatementParenthesized(
    ctx: StatementParenthesizedContext
  ): VisitorResult {
    const lparen: string[] = this.serializeSingleTerminal(ctx.LPAREN());
    const statementResult = ctx.statement().accept(this);
    const rparenStr: string[] = this.serializeSingleTerminal(ctx.RPAREN());
    const merged = this.mergeLinesWithOverlap(
      "",
      lparen,
      statementResult.sql,
      rparenStr
    );
    const sql = this.indentLines(
      merged,
      1 /* num spaces */,
      1 /* start line */
    );
    return { type: "sql", sql };
  }

  /**
   * Visit all child clauses to get their commands and parameters, then line up all commands along a river
   * and format their parameters indented to the right side of the river.
   */
  visitClauses(ctx: ClausesContext): VisitorResult {
    const command = ctx.command();
    const parameters = ctx.parameters();
    const commandsSql: string[][] = [command[0].accept(this).sql];
    const parametersSql: string[][] = [];
    for (let i = 0; i < parameters.length; i++) {
      parametersSql.push(parameters[i].accept(this).sql);
      if (i < command.length - 1) {
        commandsSql.push(command[i + 1].accept(this).sql);
      }
    }

    const sql = this.formatClauses(commandsSql, parametersSql);
    return { type: "sql", sql };
  }

  protected abstract formatClauses(
    commandsSql: string[][],
    parametersSql: string[][]
  ): string[];

  /**
   * Combine all components, separated by a space as necessary.
   */
  visitParameters(ctx: ParametersContext): VisitorResult {
    let sql: string[] = ctx.children![0].accept(this).sql;
    for (const child of ctx.children!.slice(1)) {
      const leadingSpaces =
        child instanceof SeparatorContext && child.COMMA() ? 0 : 1;
      sql = this.mergeLinesWithOverlap(
        " ".repeat(leadingSpaces),
        sql,
        child.accept(this).sql
      );
    }
    return { type: "sql", sql };
  }

  /**
   * Visit the contents to get each formatted line, and surround with parentheses. Depending on whether
   * there are tokens (other than left paren) before the body, put the body on the left paren line or
   * on the next line, indenting the contents as needed. The right paren gets put on the end token line
   * or a newline depending on the block contents and what follows it.
   */
  visitParenthesized(ctx: ParenthesizedContext): VisitorResult {
    const lparen: string[] = this.serializeSingleTerminal(ctx.LPAREN());
    const parameterLines = ctx.parameters()?.accept(this).sql || [];
    const clausesLines = ctx.clauses()?.accept(this).sql || [];
    const bodyLines = clausesLines;
    if (parameterLines.length > 0) {
      // Prefix the clauses with the leading parameter lines, adjusting them to align with
      // the column of the start of the first clause line.
      const leadingSpaces =
        bodyLines.length > 0 ? bodyLines[0].match(/^\s*/)![0].length : 0;
      bodyLines.unshift(...this.indentLines(parameterLines, leadingSpaces));
    }

    // We prefer putting contents on a new line after open paren,
    // but want to avoid a line with just the open paren and nothing else.
    const singleLineOpenAndBody = lparen.length == 1 && bodyLines.length == 1;
    const inlineContents =
      !this.hasLeadingNonOpenParenTokens(ctx) || singleLineOpenAndBody;
    const indentSpaces = inlineContents
      ? 1 /* length of the open paren */
      : SPACES_WITHIN_PAREN_BLOCK;
    const formattedlparenLines: string[] = this.indentLines(
      lparen,
      indentSpaces,
      1
    );

    let parenSql: string[] = [];
    if (inlineContents) {
      // first parenthesized group within clause: no newline after open parens
      // e.g. SELECT (col1,
      //              col2)
      const formattedBodyLines: string[] = this.indentLines(
        bodyLines,
        indentSpaces,
        1 /* start line */
      );
      parenSql.push(
        ...this.mergeLinesWithOverlap(
          "",
          formattedlparenLines,
          formattedBodyLines
        )
      );
    } else {
      // otherwise, newline after open parens
      // e.g. INSERT INTO tablename (
      //                    col1,
      //                    col2)
      const formattedBodyLines: string[] = this.indentLines(
        bodyLines,
        indentSpaces,
        0 /* start line */
      );
      parenSql.push(...formattedlparenLines);
      parenSql.push(...formattedBodyLines);
    }

    const rparen: string[] = this.serializeSingleTerminal(ctx.RPAREN());
    const isFinal = !this.hasTrailingNonCloseParenTokens(ctx);
    if (isFinal || singleLineOpenAndBody) {
      // e.g. FROM (SELECT abc
      //              FROM tbl) <--- closing paren gets put on same line
      // also ok: FROM (SELECT 1) AS tbl
      parenSql = this.mergeLinesWithOverlap("", parenSql, rparen);
    } else {
      // e.g. FROM (SELECT abc
      //              FROM tbl
      //           ) AS newtbl  <--- closing paren gets put on new line
      parenSql.push(...rparen);
    }
    return { type: "sql", sql: parenSql };
  }

  /**
   * Visit command tokens and combine them.
   */
  visitCommand(ctx: CommandContext): VisitorResult {
    const sql = this.serializeCommandTerminals(ctx.COMMAND());
    return { type: "sql", sql };
  }

  /**
   * Visit noncommand tokens and combine them.
   */
  visitNoncommand(ctx: NoncommandContext): VisitorResult {
    const sql = this.serializeTerminals(ctx.NONCOMMAND());
    return { type: "sql", sql };
  }

  /**
   * Visit a separator (comma, and, or) and add a line break after.
   */
  visitSeparator(ctx: SeparatorContext): VisitorResult {
    const separator = ctx.children![0] as TerminalNode;
    const sql = [...this.serializeSingleTerminal(separator), ""];
    return { type: "sql", sql };
  }

  visitTerminal(ctx: TerminalNode): VisitorResult {
    // Never called: parent rules manually serialize terminals.
    throw new Error("Function not implemented.");
  }

  visit(tree: ParseTree): VisitorResult {
    // Never called
    throw new Error("Function not implemented.");
  }

  visitChildren(node: RuleNode): VisitorResult {
    // Never called
    throw new Error("Function not implemented.");
  }

  visitErrorNode(node: ErrorNode): VisitorResult {
    // Never called
    throw new Error("Function not implemented.");
  }

  protected abstract formatCommands(
    commands: string[][]
  ): [formatted: string[][], maxCommandLength: number];

  protected indentLines(
    lines: string[],
    spaces: number,
    rangeStartInclusive?: number,
    rangeEndExclusive?: number
  ): string[] {
    const start = rangeStartInclusive != undefined ? rangeStartInclusive : 0;
    const end =
      rangeEndExclusive != undefined ? rangeEndExclusive : lines.length;
    const firstLines = lines.slice(0, start);
    const indentedLines = lines
      .slice(start, end)
      .map((str) => " ".repeat(spaces) + str);
    const lastLines = lines.slice(end);
    return [...firstLines, ...indentedLines, ...lastLines];
  }

  /**
   * Merge 2 or more lines, with a given separator between each (only used if we will be merging non-empty starting line).
   * E.g. separator = " ", lines1 = ["abc", "def"], lines2 = ["/* comment *\/"] -> ["abc", "def /* comment *\/"]
   */
  protected mergeLinesWithOverlap(
    separator: string,
    lines1: string[],
    ...lines2: (string[] | undefined)[]
  ): string[] {
    const merged = lines1 && lines1.length > 0 ? [...lines1] : [""];
    for (const lines of lines2) {
      if (lines && lines.length > 0) {
        const useSeparator = merged[merged.length - 1] != "";
        merged[merged.length - 1] += useSeparator // (" ", [], ["abc"]) or (" ", [""], ["abc"]) -> ["abc"] not [" abc"]
          ? separator + lines[0]
          : lines[0];
        merged.push(...lines.slice(1));
      }
    }
    return merged;
  }

  /**
   * Since we recursively build up the formatted SQL (bottom-up), when visiting a rule it isn't simple to know
   * whether there will be tokens printed by a parent/ancestor rule on the same line, previous to these.
   * Therefore, we need to infer it based on the structure of the grammar: depending on the ancestor chain
   * we can determine it.
   */
  private hasLeadingNonOpenParenTokens(ctx: ParenthesizedContext): boolean {
    let rule: RuleContext = ctx;
    let parentRule: RuleContext = ctx._parent!;
    while (!(parentRule instanceof ClausesContext)) {
      if (parentRule instanceof ParametersContext) {
        const index = parentRule.children!.indexOf(rule);
        if (
          index != 0 &&
          !(parentRule.children![index - 1] instanceof SeparatorContext)
        ) {
          return true;
        }
      } else if (parentRule instanceof ParenthesizedContext) {
        // only char before in the rule can be lparen, so just need to check its ancestors
      } else {
        throw new Error("Entered unexpected rule: " + rule.ruleIndex);
      }
      rule = parentRule;
      parentRule = parentRule._parent!;
    }
    return false;
  }

  /** See comment for hasLeadingNonOpenParenTokens */
  private hasTrailingNonCloseParenTokens(ctx: ParenthesizedContext): boolean {
    let rule: RuleContext = ctx;
    let parentRule: RuleContext = ctx._parent!;
    while (!(parentRule instanceof ClausesContext)) {
      if (parentRule instanceof ParametersContext) {
        const index = parentRule.children!.indexOf(rule);
        if (
          index != parentRule.children!.length - 1 &&
          !(parentRule.children![index + 1] instanceof SeparatorContext)
        ) {
          return true;
        }
      } else if (parentRule instanceof ParenthesizedContext) {
        // clauses will always be on newline, so just need to check its ancestors
      } else {
        throw new Error("Entered unexpected rule: " + rule.ruleIndex);
      }
      rule = parentRule;
      parentRule = parentRule._parent!;
    }
    return false;
  }

  /**
   * This method should be used to print single terminals including special tokens:
   * they should never be directly converted to strings or comments in the original SQL may be lost.
   */
  private serializeSingleTerminal(terminal: TerminalNode): string[] {
    return this.serializeTerminals([terminal]);
  }

  private serializeCommandTerminals(terminals: TerminalNode[]): string[] {
    return this.serializeTerminals(terminals, true);
  }

  protected getNumSpacesAfterCommand(): number {
    return SPACES_AFTER_COMMAND;
  }

  /**
   * This will return a single-element array UNLESS there are comments as those may be put on their own line(s)
   * depending on the positioning of the comment relative to the token that precedes it.
   * We map each terminal to the original text parsed by DremioParser, adding any hidden tokens that appear after
   * each terminal.
   */
  private serializeTerminals(
    terminals: TerminalNode[],
    areCommandTokens?: boolean
  ): string[] {
    let serialized = [""];
    for (let i = 0; i < terminals.length; i++) {
      const terminal = terminals[i];
      const originalTokenInfo = this.getOriginalToken(terminal);
      const addLeadingSpace =
        originalTokenInfo.requiresLeadingSpace &&
        serialized[serialized.length - 1] != "";
      if (addLeadingSpace) {
        serialized[serialized.length - 1] += " ";
      }
      serialized[serialized.length - 1] += originalTokenInfo.text;

      const hiddenTokensAfter = originalTokenInfo.hiddenTokensAfter;
      const commentsAfter = this.serializeHiddenTokensAfter(
        terminal,
        hiddenTokensAfter
      );
      if (commentsAfter.length > 1 || commentsAfter[0] != "") {
        const separatorSpaces = areCommandTokens
          ? this.getNumSpacesAfterCommand()
          : 1;
        if (commentsAfter[0] != "") {
          serialized[serialized.length - 1] +=
            " ".repeat(separatorSpaces) + commentsAfter[0];
        }
        serialized.push(...commentsAfter.slice(1));
        if (areCommandTokens && commentsAfter[commentsAfter.length - 1] != "") {
          serialized.push(""); // Within a command, no command tokens follow comments on same line
        }
      }
    }
    return serialized;
  }

  private getOriginalToken(terminal: TerminalNode): OriginalTokenInfo {
    let tokenIndexInStatement = terminal.symbol.tokenIndex;
    let statementNum = 0;
    let nextStatementStartIdx = this.originalTokensInfo[0].length;
    // E.g. for query SELECT 1;; SELECT 1; SELECT 2
    // If terminal=2: raw index is 7. Final statementNum=2, tokenIndexInStatement=1
    while (tokenIndexInStatement >= nextStatementStartIdx) {
      tokenIndexInStatement -= nextStatementStartIdx;
      statementNum++;
      nextStatementStartIdx = this.originalTokensInfo[statementNum].length;
    }
    return this.originalTokensInfo[statementNum][tokenIndexInStatement];
  }

  private serializeHiddenTokensAfter(
    terminal: TerminalNode,
    hiddenTokensAfter: HiddenTokens
  ): string[] {
    let prevTerminalOriginalLineNum: number =
      this.getOriginalToken(terminal).lineNum;
    return this.serializeHiddenTokens(
      hiddenTokensAfter,
      prevTerminalOriginalLineNum
    );
  }

  /**
   * This method is responsible for stitching together hidden tokens (comments). We take into account the line
   * number they appear on (in relation to the previous token's line number) as a heuristic to try to preserve
   * positioning of the comments (e.g. on same line as the previous token, on separate line). We must also make
   * sure to add a blank line after a final single line comment so that subsequent terminals aren't effectively
   * "commented out". (--blah SELECT 1)
   */
  private serializeHiddenTokens(
    hiddenTokens: HiddenTokens,
    prevTerminalOriginalLineNum?: number
  ): string[] {
    let hiddenTokensLines: string[] = [""];
    let lastTokenOriginalLineNum: number = prevTerminalOriginalLineNum || -1;
    for (let i = 0; i < hiddenTokens.length; i++) {
      const [
        hiddenTokenRawText,
        hiddenTokenLineNum,
        hiddenTokenStartCol,
        hiddenType,
      ] = hiddenTokens[i];
      const isSingleLineComment =
        hiddenType == HiddenTokenType.SINGLE_LINE_COMMENT;
      // If it is multi line style token it might span multiple lines -> convert to individual
      // lines, adjusting the leading whitespace on trailing lines based on the col idx of the token start
      // E.g. "/*\n       * comment\n       */" -> ["/*", " * comment", "*/"] if comment start idx was 6
      // If it is weird e.g. the trailing lines start farther left than the token open, we can't preserve that
      // Caller responsible for converting tabs to spaces before parsing the original SQL, or we wouldn't do this well
      const hiddenTokenText: string[] = hiddenTokenRawText.split("\n");
      for (let i = 1; i < hiddenTokenText.length; i++) {
        for (let j = 0; j < hiddenTokenStartCol; j++) {
          if (hiddenTokenText[i].startsWith(" ")) {
            hiddenTokenText[i] = hiddenTokenText[i].replace(" ", "");
          } else {
            break;
          }
        }
      }
      // If hidden tokens consist of multiple lines, start it on its own line because it won't look good if
      // we don't adjust secondary lines leading spaces to match original relative positioning
      // E.g. SELECT abcd /* hello      convert to   SELECT abcd       since better than   SELECT abcd /* hello
      //                     world */                       /* hello                               world */
      //                                                       world */

      // If hidden token originally came on line after last token (hidden or not), prevent putting them on the same line
      // E.g. SELECT a,          and not   SELECT a, /* b */ /* c */
      //             /* b, */
      //             /* c, */
      if (hiddenTokenLineNum > lastTokenOriginalLineNum) {
        hiddenTokensLines.push(...hiddenTokenText);
      } else {
        hiddenTokensLines = this.mergeLinesWithOverlap(
          " ",
          hiddenTokensLines,
          hiddenTokenText
        );
      }
      if (isSingleLineComment) {
        hiddenTokensLines.push(""); // any following terminals (produced here or later) need to be on new line
      }
      lastTokenOriginalLineNum = hiddenTokenLineNum;
    }
    return hiddenTokensLines;
  }
}
