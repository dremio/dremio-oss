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

import { QueryRange } from "#oss/utils/statements/statement";

export type RawErrorInfo = {
  readonly message: string;
  readonly range?: QueryRangeResponse;
};

export type ErrorResponse = {
  readonly errorMessage?: string;
  readonly details?: {
    readonly errors?: RawErrorInfo[];
  };
  readonly range?: QueryRangeResponse;
};

export type QueryRangeResponse = {
  readonly startLine: number;
  readonly startColumn: number;
  readonly endLine: number;
  readonly endColumn: number;
};

export type SqlError = {
  readonly message: string;
  readonly range: QueryRange;
};

export const DEFAULT_ERROR_MESSAGE = "Error";

/**
 * Error message will be in the only error within the error details.
 * NOTE: if this behavior changes on the server, this is the place to update.
 */
const getSqlError = (
  errorResponse: ErrorResponse | undefined,
): RawErrorInfo | undefined => {
  return errorResponse?.details?.errors?.[0];
};

/**
 * Given an absolute range of the full statement and a range of an error within that statement,
 * returns an absolute range of an error
 */
const getErrorRange = (
  statementRange: QueryRange,
  relativeErrorRange: QueryRangeResponse | undefined,
): QueryRange => {
  if (relativeErrorRange === undefined) {
    return statementRange;
  }
  const lineOffset = statementRange.startLineNumber - 1; // lines are 1-based, but offsets are 0-based
  const columnOffset = statementRange.startColumn - 1; // same here

  // Two observations:
  // 1. start and end lines within the error will be offset by the line offset
  // 2. columns on the FIRST LINE ONLY will be offset by the column offset
  return {
    startLineNumber: relativeErrorRange.startLine + lineOffset,
    endLineNumber: relativeErrorRange.endLine + lineOffset,
    startColumn:
      relativeErrorRange.startLine === 1
        ? relativeErrorRange.startColumn + columnOffset
        : relativeErrorRange.startColumn,
    // monaco looks at how to draw error by using end column as an exclusive index, hence +1
    endColumn:
      relativeErrorRange.startLine === 1
        ? relativeErrorRange.endColumn + columnOffset + 1
        : relativeErrorRange.endColumn + 1,
  };
};

/**
 *
 * @param errorResponse response from the server which contains either details error or a generic one
 * @param queryRange range within sql editor of where the error happened
 * @returns error message along with a (potentially) narrowed down range of where the error happened within the editor.
 */
export const extractSqlErrorFromResponse = (
  errorResponse: ErrorResponse | undefined,
  queryRange: QueryRange,
): SqlError => {
  const sqlError = getSqlError(errorResponse);
  if (!sqlError) {
    return {
      message: errorResponse?.errorMessage ?? DEFAULT_ERROR_MESSAGE,
      range: errorResponse?.range
        ? getErrorRange(queryRange, errorResponse.range)
        : queryRange,
    };
  }
  return {
    message: sqlError.message,
    range: getErrorRange(queryRange, sqlError.range),
  };
};
