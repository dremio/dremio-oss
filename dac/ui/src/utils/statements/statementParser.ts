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

import {
  advancePosition,
  isPastLastPosition,
  Position,
  positionOf,
  symbolAt,
} from "./position";
import { charIfExists } from "./stringUtils";
import { extractSql, QueryRange, Statement, toQueryRange } from "./statement";
import { isSkipped, skip, Skippable, value } from "./skippable";

type SingleCharacterPart = {
  readonly kind: "new-line" | "semicolon" | "non-eol-whitespace";
  readonly position: Position;
};

type QueryPart =
  | SingleCharacterPart
  | {
      readonly kind:
        | "single-line-comment"
        | "multi-line-comment"
        | "expression";
      readonly from: Position;
      // to is inclusive
      readonly to: Position;
    };

// Helpers to detect different query parts
const isNewLine = (query: string, position: Position): boolean =>
  symbolAt(query, position) === "\n";

const isSemicolon = (query: string, position: Position): boolean =>
  symbolAt(query, position) === ";";

const isWhitespace = (query: string, position: Position): boolean =>
  /\s/.test(symbolAt(query, position));

const isSingleLineComment = (query: string, position: Position): boolean =>
  (symbolAt(query, position) === "-" &&
    charIfExists(query, position.index + 1) === "-") ||
  (symbolAt(query, position) === "/" &&
    charIfExists(query, position.index + 1) === "/");

const isMultiLineComment = (query: string, position: Position): boolean =>
  symbolAt(query, position) === "/" &&
  charIfExists(query, position.index + 1) === "*";
const isMultiLineCommentClose = (query: string, position: Position): boolean =>
  symbolAt(query, position) === "*" &&
  charIfExists(query, position.index + 1) === "/";

const isNonExpression = (query: string, position: Position): boolean =>
  isNewLine(query, position) ||
  isWhitespace(query, position) ||
  isSemicolon(query, position) ||
  isSingleLineComment(query, position) ||
  isMultiLineComment(query, position);

/**
 * Returns the next query part that starts with a given position.
 * The method returns eof if "from" position is out of bounds
 */
const extractQueryPart = (query: string, from: Position): QueryPart => {
  if (isNewLine(query, from)) {
    return {
      kind: "new-line",
      position: from,
    };
  }
  if (isSemicolon(query, from)) {
    return {
      kind: "semicolon",
      position: from,
    };
  }
  if (isWhitespace(query, from)) {
    return {
      kind: "non-eol-whitespace",
      position: from,
    };
  }
  if (isSingleLineComment(query, from)) {
    return extractSingleLineComment(query, from);
  }
  if (isMultiLineComment(query, from)) {
    return extractMultiLineComment(query, from);
  }
  return extractExpression(query, from);
};

/**
 * Extracts single line comment. Assumed that "from" points to the initial "--"
 * Includes eol into the comment.
 */
const extractSingleLineComment = (query: string, from: Position): QueryPart => {
  // Advances "from" to a new index, assumes NO new lines were encountered
  const advanceToIndex = (newIndex: number) =>
    positionOf(advancePosition(query, from, newIndex - from.index));

  const newLineIndex = query.indexOf("\n", from.index);
  const to =
    newLineIndex === -1
      ? advanceToIndex(query.length - 1)
      : advanceToIndex(newLineIndex);
  return {
    kind: "single-line-comment",
    from,
    to,
  };
};

/**
 * Extracts multi-line comment. Assumed that "from" points to the initial "/*"
 * If comment is not closed until the eof, last symbol position is returned
 */
const extractMultiLineComment = (query: string, from: Position): QueryPart => {
  let positionCursor = advancePosition(query, from, 2);

  while (!isPastLastPosition(positionCursor)) {
    const currentPosition = positionCursor.value;
    if (isMultiLineCommentClose(query, currentPosition)) {
      return {
        kind: "multi-line-comment",
        from,
        to: positionOf(advancePosition(query, currentPosition, 1)), // point at "/"
      };
    }
    positionCursor = advancePosition(query, currentPosition, 1);
  }
  // if we are here, we reached the last position
  return {
    kind: "multi-line-comment",
    from,
    to: positionCursor.to,
  };
};

/**
 * If a symbol at a position denotes the start of a literal or an identifier, returns matching close syumbol
 * undefined otherwise.
 */
const getMatchingQuoteForLiteralOrIdentifierAt = (
  query: string,
  position: Position
): string | undefined => {
  const char = symbolAt(query, position);
  switch (char) {
    case "`":
    case '"':
    case "'":
      return char;
    case "[":
      return "]";
  }
  return undefined;
};

/**
 * Returns position of the quote symbol ignoring escaped ones.
 * If end of input is reached,last symbol is returned.
 * @param quote Closing quote to match
 * @param position position of the opening quote
 */
const advanceToClosingQuote = (
  query: string,
  quote: string,
  position: Position
): Position => {
  let positionCursor = advancePosition(query, position, 1); // skipping opening quote

  while (!isPastLastPosition(positionCursor)) {
    const currentPosition = positionCursor.value;
    const currentSymbol = symbolAt(query, currentPosition);
    if (currentSymbol === quote) {
      // make sure it is not escaped and return current position if not
      if (charIfExists(query, currentPosition.index + 1) !== quote) {
        return currentPosition;
      }
      // skip escaped symbol altogether
      positionCursor = advancePosition(query, currentPosition, 2);
    } else {
      positionCursor = advancePosition(query, currentPosition, 1);
    }
  }
  // we reached eof
  return positionCursor.to;
};

/**
 * Extracts regular sql part but only until the new line.
 * Expression here refers to any meaningful text that can be interpreted as part of a sql statement.
 * For example, whitespaces, comments, semicolons are not considered as contributing to the expression
 * (even though they can be part of it)
 * From is guaranteed to belong to a regular sql, so at least one character is always returned.
 */
const extractExpression = (query: string, from: Position): QueryPart => {
  // we need to track both because sometimes we need to return previous position
  // and adbvancePosition is not reversible.
  let lastPosition = from;
  let positionCursor: Skippable<Position> = value(from);

  while (!isPastLastPosition(positionCursor)) {
    const currentPosition = positionCursor.value;
    if (isNonExpression(query, currentPosition)) {
      return {
        kind: "expression",
        from,
        to: lastPosition, // return previous position
      };
    }

    const matchingQuote = getMatchingQuoteForLiteralOrIdentifierAt(
      query,
      currentPosition
    );
    if (matchingQuote !== undefined) {
      lastPosition = advanceToClosingQuote(
        query,
        matchingQuote,
        currentPosition
      );
    } else {
      lastPosition = currentPosition;
    }
    positionCursor = advancePosition(query, lastPosition, 1);
  }
  // if we are here, we reached eof
  return {
    kind: "expression",
    from,
    to: positionCursor.to,
  };
};

/**
 * Skips semicolons starting from the current positions and returns
 * a position of the first non-semicolon statement.
 * If such position doesn't exist, undefined is returned.
 */
const skipSemicolons = (
  query: string,
  positionCursor: Skippable<Position>
): Skippable<Position> => {
  if (isPastLastPosition(positionCursor)) {
    return positionCursor;
  }
  const position = positionCursor.value;
  if (isSemicolon(query, position)) {
    return skipSemicolons(query, advancePosition(query, position, 1));
  }
  return value(position);
};

/**
 * extracts the first non-empty statement starting with a position.
 * If such statement doesn't exist, undefined is returned.
 * Empty statement is a statement that consists only of comments or whitespaces
 */
const extractStatement = (
  query: string,
  from: Position
): Skippable<Statement> => {
  const startPosition = skipSemicolons(query, value(from));
  if (isSkipped(startPosition)) {
    return startPosition;
  }
  // will track if we saw any "meaningful" query part when extracting the statement
  let containsSql = false;

  let positionCursor: Skippable<Position> = startPosition;
  let currentTo = startPosition.value;
  while (
    !isPastLastPosition(positionCursor) &&
    !isSemicolon(query, positionCursor.value)
  ) {
    const currentPosition = positionCursor.value;
    const currentPart = extractQueryPart(query, currentPosition);

    switch (currentPart.kind) {
      case "new-line":
      case "non-eol-whitespace":
        currentTo = currentPart.position;
        break;
      case "single-line-comment":
      case "multi-line-comment":
        currentTo = currentPart.to;
        break;
      case "expression":
        currentTo = currentPart.to;
        containsSql = true;
        break;
    }
    positionCursor = advancePosition(query, currentTo, 1);
  }

  // once we got here:
  //  - currentTo points to the last part's end that we processed
  //  - we either at the end of input OR saw a semicolon
  // we can also check if we saw a single sql part, and if not just return undefined.
  if (!containsSql) {
    return skip(currentTo);
  }
  return value({
    from: startPosition.value,
    to: currentTo,
  });
};

export const extractStatements = (
  query: string | null | undefined
): Statement[] => {
  if (query === null || query === undefined || query.length === 0) {
    return [];
  }
  const statements: Statement[] = [];

  let positionCursor: Skippable<Position> = value({
    column: 1,
    line: 1,
    index: 0,
    isLineBreak: query[0] === "\n",
  });

  while (!isPastLastPosition(positionCursor)) {
    const statement = extractStatement(query, positionCursor.value);
    if (isSkipped(statement)) {
      positionCursor = advancePosition(query, statement.to, 1);
    } else {
      statements.push(statement.value);
      // this is guaranteed to either point to the last symbol
      // or semicolon if there are more statements
      positionCursor = advancePosition(query, statement.value.to, 1);
    }
  }
  return statements;
};

export const extractSelections = (query: string): QueryRange[] =>
  extractStatements(query).map(toQueryRange);

export const extractQueries = (query: string): string[] =>
  extractStatements(query).map((s) => extractSql(query, s));
