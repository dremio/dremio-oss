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

export const SqlStringUtils = (sqlQuery: string): string[] => {
  // base case, empty string
  if (!sqlQuery) return [];

  const textLenght = sqlQuery.length;
  const result = [] as string[];

  let start = 0;

  for (let i = 0; i < textLenght; i++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      // End of statement, add the buffer to the list
      case ';':
        // ignore
        if (start == i) break;

        const statement = sqlQuery.substring(start, i);
        result.push(statement);
        start = i + 1;
        break;

      // Identifiers and quoted strings
      case '\'':
      case '"':
      case '`':
      case '[':
        const endQuote = char != '[' ? char : ']';
        const endQuoteIndex = indexOfEndOfQuote(sqlQuery, endQuote, i + 1);
        i = endQuoteIndex;
        break;

      // Comments
      // @ts-ignore, ignore a warning about fallthroughs
      case '/':
        // handle multi-line comment
        if (i + 1 < textLenght && sqlQuery.charAt(i + 1) === '*') {
          const endOfCommentIndex = indexOfEndOfMultiLineComment(
            sqlQuery,
            i + 2
          );
          i = endOfCommentIndex;
        }
      // fallthrough

      case '-':
        // maybe the start of a single line comment
        if (i + 1 < textLenght && sqlQuery.charAt(i + 1) === char) {
          const endOfCommentIndex = indexOfEndOfSingleLineComment(
            sqlQuery,
            i + 2
          );
          i = endOfCommentIndex;
        }

        break;

      default:
        break;
    }
  }

  // Last element, if any
  if (start !== textLenght) {
    const statement = sqlQuery.substring(start);
    result.push(statement);
  }

  return result;
};

const indexOfEndOfQuote = (
  sqlQuery: string,
  quoteChar: string,
  fromIndex: number
): number => {
  const textLenght = sqlQuery.length;

  let start = fromIndex;
  while (true) {
    const indexOf = sqlQuery.indexOf(quoteChar, start);

    // invalid SQL string
    if (indexOf === -1) return textLenght;

    if (indexOf == textLenght || sqlQuery.charAt(indexOf + 1) !== quoteChar) {
      return indexOf;
    }

    // escape char, ignore
    start = indexOf + 2;
  }
};

const indexOfEndOfMultiLineComment = (
  sqlQuery: string,
  fromIndex: number
): number => {
  const textLenght = sqlQuery.length;

  for (let i = fromIndex; i < textLenght; i++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      case '*':
        if (i + 1 < textLenght && sqlQuery.charAt(i + 1) === '/') return i + 1;
        break;

      default:
        break;
    }
  }

  // EOT
  return textLenght;
};

const indexOfEndOfSingleLineComment = (
  sqlQuery: string,
  fromIndex: number
): number => {
  const textLenght = sqlQuery.length;

  for (let i = fromIndex; i < textLenght; i++) {
    const char = sqlQuery.charAt(i);

    switch (char) {
      case '\r':
        if (i + 1 < textLenght && sqlQuery.charAt(i + 1) === '\n') return i + 1;
        return i;

      case '\n':
        return i;

      default:
        break;
    }
  }

  // EOT
  return textLenght;
};
