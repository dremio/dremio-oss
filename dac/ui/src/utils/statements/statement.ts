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

import { Position } from "./position";

export type Statement = {
  readonly from: Position;
  readonly to: Position;
};

/** All values 1-indexed */
export type QueryRange = {
  readonly startLineNumber: number;
  readonly startColumn: number;
  readonly endLineNumber: number;
  readonly endColumn: number;
};

export const extractSql = (query: string, statement: Statement): string =>
  query.substring(statement.from.index, statement.to.index + 1);

export const toQueryRange = (statement: Statement): QueryRange => {
  return {
    startLineNumber: statement.from.line,
    startColumn: statement.from.column,
    endLineNumber: statement.to.line,
    endColumn: statement.to.column + 1, // query range has exclusive boundaries
  };
};
