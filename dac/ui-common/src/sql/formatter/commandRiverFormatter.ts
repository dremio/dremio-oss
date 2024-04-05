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

import { AbstractSqlAstVisitor } from "./abstractSqlAstVisitor";

export abstract class CommandRiverFormatter extends AbstractSqlAstVisitor {
  protected abstract formatCommandLine(
    line: string,
    commandLength: number,
    maxCommandLength: number,
  ): string;

  protected formatClauses(
    commandsSql: string[][],
    parametersSql: string[][],
  ): string[] {
    const [commandsAsRiver, maxCommandLength] =
      this.formatCommands(commandsSql);
    const parameterLinesLeadingSpaces =
      maxCommandLength + this.getNumSpacesAfterCommand();
    let sql: string[] = [];
    for (let i = 0; i < commandsAsRiver.length; i++) {
      const parametersSqlLines =
        i < parametersSql.length ? parametersSql[i] : [];
      const commandAsRiver: string[] = commandsAsRiver[i];
      const indentedParameterLines = this.indentLines(
        parametersSqlLines,
        parameterLinesLeadingSpaces,
        1 /* start line */,
      );
      const clauseSql = this.mergeLinesWithOverlap(
        " ".repeat(this.getNumSpacesAfterCommand()),
        commandAsRiver,
        indentedParameterLines,
      );
      sql.push(...clauseSql);
    }
    return sql;
  }

  // Normal case:
  // [SHOW LOGS AT TAG] ==> [SHOW LOGS AT TAG]
  // [IN]                   [              IN]
  //
  // Abnormal case:
  // [SHOW LOGS --blah      [SHOW LOGS --blah
  //  --blah                           --blah
  //  AT TAG -- blah    ==>     AT TAG --blah
  // ]                      ]
  // [IN]                   [       IN]
  // Command could be multi line if there was 1 or more single line comments between or after the command tokens
  protected formatCommands(
    commands: string[][],
  ): [formatted: string[][], maxCommandLength: number] {
    let maxCommandLength = 0;
    const getCommandWords = (line: string) => line.split(/\s*[^\w\s]/, 1)[0]; // DROP USER --blah or DROP USER /* blah */ -> DROP USER
    for (const command of commands) {
      for (const line of command) {
        // Might be a trailing blank line (preceded by single line comment) or single line comment line
        // Those do not impact the river start column
        const commandWords: string = getCommandWords(line);
        maxCommandLength = Math.max(maxCommandLength, commandWords.length);
      }
    }
    const formattedCommands: string[][] = [];
    for (const command of commands) {
      const formattedLines: string[] = [];
      for (const line of command) {
        const commandWords: string = getCommandWords(line);
        const areCommandTokens = commandWords != "";
        if (!line) {
          // Preserve trailing blank lines (e.g. that come after single line comment after last command token)
          formattedLines.push(" ".repeat(maxCommandLength));
        } else if (!areCommandTokens) {
          // Format these comment-only lines on right side of the river
          formattedLines.push(
            " ".repeat(maxCommandLength) +
              " ".repeat(this.getNumSpacesAfterCommand()) +
              line,
          );
        } else {
          // Normal case: Add leading spaces so all command token of each clause across the entire statement
          // line up along the river
          formattedLines.push(
            this.formatCommandLine(line, commandWords.length, maxCommandLength),
          );
        }
      }
      formattedCommands.push(formattedLines);
    }
    return [formattedCommands, maxCommandLength];
  }
}
