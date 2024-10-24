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

export enum TableColumns {
  TEXT = "TEXT",
  VARCHAR = "VARCHAR",
  BINARY = "BINARY",
  VARBINARY = "VARBINARY",
  BOOLEAN = "BOOLEAN",
  FLOAT = "FLOAT",
  DECIMAL = "DECIMAL",
  INTEGER = "INTEGER",
  DOUBLE = "DOUBLE",
  BIGINT = "BIGINT",
  MIXED = "MIXED",
  UNION = "UNION",
  DATE = "DATE",
  TIME = "TIME",
  DATETIME = "DATETIME",
  TIMESTAMP = "TIMESTAMP",
  LIST = "LIST",
  MAP = "MAP",
  STRUCT = "STRUCT",
  GEO = "GEO",
  OTHER = "OTHER",
  ANY = "ANY",
}

export const TableColumnsIcons = {
  [TableColumns.TEXT]: "TypeText",
  [TableColumns.VARCHAR]: "TypeText",
  [TableColumns.BINARY]: "TypeBinary",
  [TableColumns.VARBINARY]: "TypeBinary",
  [TableColumns.BOOLEAN]: "TypeBoolean",
  [TableColumns.FLOAT]: "TypeFloat",
  [TableColumns.DECIMAL]: "TypeDecimal",
  [TableColumns.INTEGER]: "TypeInteger",
  [TableColumns.DOUBLE]: "TypeFloat",
  [TableColumns.BIGINT]: "TypeInteger",
  [TableColumns.MIXED]: "TypeMixed",
  [TableColumns.UNION]: "TypeMixed",
  [TableColumns.DATE]: "Date",
  [TableColumns.TIME]: "Time",
  [TableColumns.DATETIME]: "TypeDateTime",
  [TableColumns.TIMESTAMP]: "TypeDateTime",
  [TableColumns.LIST]: "TypeList",
  [TableColumns.MAP]: "TypeMap",
  [TableColumns.STRUCT]: "TypeStruct",
  [TableColumns.GEO]: "TypeGeo",
  [TableColumns.OTHER]: "TypeOther",
  [TableColumns.ANY]: "TypeOther",
};
