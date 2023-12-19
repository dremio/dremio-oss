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

import type { SimpleDataType } from "../../../sonar/catalog/SimpleDataType.type";
import type { ContainerType } from "./ContainerType";

export type TableColumn = {
  origin: "table";
  name: string;
  type: SimpleDataType;
};
export type ExpressionColumn = {
  origin: "expression";
  name: string;
};
export type Column = TableColumn | ExpressionColumn;

export type TableBase = {
  columns: Column[];
  alias?: string;
  columnAliases?: string[];
};
export type RegularTable = TableBase & {
  derived: false;
  path: string[];
  type:
    | typeof ContainerType.DIRECT
    | typeof ContainerType.PROMOTED
    | typeof ContainerType.VIRTUAL;
};
export type DerivedTable = TableBase & {
  derived: true;
};

export type Table = RegularTable | DerivedTable;
