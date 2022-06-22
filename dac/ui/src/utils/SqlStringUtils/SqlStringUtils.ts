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

import { cloneDeep } from "lodash";

export const SqlStringUtils = (sqlQuery: string): any => {
  // base case, empty string
  if (!sqlQuery) return [[], []];

  const textLength = sqlQuery.length;
  const result = [] as string[];
  const selections = [] as any[];

  let start = 0;
  let endAsComment = false;

  const selection = {
    endColumn: 1,
    endLineNumber: 1,
    positionColumn: 1,
    positionLineNumber: 1,
    selectionStartColumn: 1,
    selectionStartLineNumber: 1,
    startColumn: 1,
    startLineNumber: 1,
  };

  for (let i = 0; i < textLength; i++, selection.positionColumn++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      // End of statement, add the buffer to the list
      case ";": {
        // ignore
        if (start === i) {
          start = i + 1;
          break;
        }

        selection.endLineNumber = selection.positionLineNumber;
        selection.endColumn = selection.positionColumn;
        selection.selectionStartColumn = selection.startColumn;
        selection.selectionStartLineNumber = selection.startLineNumber;

        const newSelection = cloneDeep({
          ...selection,
          endColumn: selection.positionColumn + 1,
          positionColumn: selection.positionColumn + 1,
        });
        selections.push(newSelection);

        const statement = sqlQuery.substring(start, i);
        result.push(statement);
        start = i + 1;
        selection.startColumn = selection.positionColumn + 1;
        selection.startLineNumber = selection.positionLineNumber;
        break;
      }

      // Spaces or special characters at the beginning of queries
      case " ":
        if (start === i) {
          start = i + 1;
          selection.startColumn = selection.positionColumn + 1;
          selection.startLineNumber = selection.positionLineNumber;
        }
        break;

      // Identifiers and quoted strings
      case "'":
      case '"':
      case "`":
      case "[": {
        const endQuote = char != "[" ? char : "]";
        const endQuoteIndex = indexOfEndOfQuote(sqlQuery, endQuote, i + 1);
        const diff = endQuoteIndex - i;
        selection.positionColumn = selection.positionColumn + diff;
        i = endQuoteIndex;
        break;
      }

      // Comments
      case "/":
        // handle multi-line comment
        if (i + 1 < textLength && sqlQuery.charAt(i + 1) === "*") {
          const [endOfCommentIndex, addNewLines] = indexOfEndOfMultiLineComment(
            sqlQuery,
            i + 2
          );
          const diff = endOfCommentIndex - i;
          selection.positionColumn = selection.positionColumn + diff;
          if (addNewLines > 0) {
            selection.positionLineNumber =
              selection.positionLineNumber + addNewLines;
          }
          i = endOfCommentIndex;
          if (
            endOfCommentIndex >= textLength ||
            endsInWhitepace(sqlQuery, endOfCommentIndex + 1)
          ) {
            endAsComment = true;
          }
        } else if (i + 1 < textLength && sqlQuery.charAt(i + 1) === "/") {
          const endOfCommentIndex = indexOfEndOfSingleLineComment(
            sqlQuery,
            i + 2
          );
          const diff = endOfCommentIndex - i;
          i = endOfCommentIndex;
          if (
            endOfCommentIndex >= textLength ||
            endsInWhitepace(sqlQuery, endOfCommentIndex)
          ) {
            endAsComment = true;
            selection.positionColumn = selection.positionColumn + diff;
          } else {
            selection.positionColumn = 0;
            selection.positionLineNumber = selection.positionLineNumber + 1;
          }
        }

        break;
      // fallthrough

      case "-":
        // maybe the start of a single line comment
        if (i + 1 < textLength && sqlQuery.charAt(i + 1) === char) {
          const endOfCommentIndex = indexOfEndOfSingleLineComment(
            sqlQuery,
            i + 2
          );
          const diff = endOfCommentIndex - i;
          i = endOfCommentIndex;
          if (
            endOfCommentIndex >= textLength ||
            endsInWhitepace(sqlQuery, endOfCommentIndex)
          ) {
            endAsComment = true;
            selection.positionColumn = selection.positionColumn + diff;
          } else {
            selection.positionColumn = 0;
            selection.positionLineNumber = selection.positionLineNumber + 1;
          }
        }

        break;

      case "\n":
        if (start === i) {
          start = i + 1;
        }
        if (i > 0 && sqlQuery.charAt(i - 1) === ";") {
          selection.startColumn = 1;
          selection.startLineNumber = selection.positionLineNumber + 1;
        }
        selection.positionLineNumber = selection.positionLineNumber + 1;
        selection.positionColumn = 0;
        break;

      default:
        break;
    }
  }

  // Last element, if any
  if (start !== textLength) {
    const statement = sqlQuery.substring(start);
    const lastStatementExists =
      !statement.startsWith("-") && !statement.startsWith("/");

    if (endAsComment && !lastStatementExists && result.length) {
      selection.endLineNumber = selection.positionLineNumber;
      selection.endColumn = selection.positionColumn;
      selection.startLineNumber = selection.selectionStartLineNumber;

      const lastResult = result[result.length - 1];
      result[result.length - 1] = lastResult + "\n" + statement;

      const lastSelection = selections[selections.length - 1];
      selections[selections.length - 1] = {
        ...lastSelection,
        endColumn: selection.endColumn,
        positionColumn: selection.positionColumn,
        endLineNumber: selection.endLineNumber,
        positionLineNumber: selection.positionLineNumber,
      };
    } else {
      selection.endLineNumber = selection.positionLineNumber;
      selection.endColumn = selection.positionColumn;
      selection.selectionStartColumn = selection.startColumn;
      selection.selectionStartLineNumber = selection.startLineNumber;

      const newSelection = cloneDeep({
        ...selection,
        endColumn: selection.positionColumn + 1,
        positionColumn: selection.positionColumn + 1,
      });

      result.push(statement);
      selections.push(newSelection);
    }
  }

  return [result, selections];
};

const indexOfEndOfQuote = (
  sqlQuery: string,
  quoteChar: string,
  fromIndex: number
): number => {
  const textLength = sqlQuery.length;

  let start = fromIndex;
  for (;;) {
    const indexOf = sqlQuery.indexOf(quoteChar, start);

    // invalid SQL string
    if (indexOf === -1) return textLength;

    if (indexOf === textLength || sqlQuery.charAt(indexOf + 1) !== quoteChar) {
      return indexOf;
    }

    // escape char, ignore
    start = indexOf + 2;
  }
};

const indexOfEndOfMultiLineComment = (
  sqlQuery: string,
  fromIndex: number
): [number, number] => {
  const textLength = sqlQuery.length;
  let addNewLines = 0;

  for (let i = fromIndex; i < textLength; i++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      case "*":
        if (i + 1 < textLength && sqlQuery.charAt(i + 1) === "/")
          return [i + 1, addNewLines];
        break;
      case "\n":
        addNewLines++;
        break;

      default:
        break;
    }
  }

  // EOT
  return [textLength, addNewLines];
};

const indexOfEndOfSingleLineComment = (
  sqlQuery: string,
  fromIndex: number
): number => {
  const textLength = sqlQuery.length;

  for (let i = fromIndex; i < textLength; i++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      case "\r":
        if (i + 1 < textLength && sqlQuery.charAt(i + 1) === "\n") return i + 1;
        return i;

      case "\n":
        return i;

      default:
        break;
    }
  }

  // EOT
  return textLength;
};

// used to check if ending comments are followed by whitespace characters
const endsInWhitepace = (sqlQuery: string, fromIndex: number) => {
  const textLength = sqlQuery.length;

  for (let i = fromIndex; i < textLength; i++) {
    if (![" ", "\n", "\r"].includes(sqlQuery.charAt(i))) {
      return false;
    }
  }

  return true;
};
