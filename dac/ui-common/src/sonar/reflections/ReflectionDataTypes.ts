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

export enum ReflectionDataType {
  DECIMAL = "DECIMAL",
  INTEGER = "INTEGER",
  BIGINT = "BIGINT",
  FLOAT = "FLOAT",
  DOUBLE = "DOUBLE",
  VARCHAR = "VARCHAR",
  VARBINARY = "VARBINARY",
  BOOLEAN = "BOOLEAN",
  DATE = "DATE",
  TIME = "TIME",
  TIMESTAMP = "TIMESTAMP",
  INTERVAL = "INTERVAL",
  STRUCT = "STRUCT",
  LIST = "LIST",
  MAP = "MAP",
}

export enum PartitionTransformations {
  IDENTITY = "IDENTITY",
  BUCKET = "BUCKET",
  TRUNCATE = "TRUNCATE",
  HOUR = "HOUR",
  DAY = "DAY",
  MONTH = "MONTH",
  YEAR = "YEAR",
}

const PartitionNone = [] as const;

const PartitionIdentity = [PartitionTransformations.IDENTITY] as const;

const PartitionBucketTruncate = [
  PartitionTransformations.IDENTITY,
  PartitionTransformations.BUCKET,
  PartitionTransformations.TRUNCATE,
] as const;

const PartitionDate = [
  PartitionTransformations.IDENTITY,
  PartitionTransformations.BUCKET,
  PartitionTransformations.DAY,
  PartitionTransformations.MONTH,
  PartitionTransformations.YEAR,
] as const;

const PartitionTimestamp = [
  PartitionTransformations.IDENTITY,
  PartitionTransformations.BUCKET,
  PartitionTransformations.HOUR,
  PartitionTransformations.DAY,
  PartitionTransformations.MONTH,
  PartitionTransformations.YEAR,
] as const;

export const DataTypeTransformations = {
  [ReflectionDataType.DECIMAL]: PartitionBucketTruncate,
  [ReflectionDataType.INTEGER]: PartitionBucketTruncate,
  [ReflectionDataType.BIGINT]: PartitionBucketTruncate,
  [ReflectionDataType.FLOAT]: PartitionIdentity,
  [ReflectionDataType.DOUBLE]: PartitionIdentity,
  [ReflectionDataType.VARCHAR]: PartitionBucketTruncate,
  [ReflectionDataType.VARBINARY]: PartitionBucketTruncate,
  [ReflectionDataType.BOOLEAN]: PartitionIdentity,
  [ReflectionDataType.DATE]: PartitionDate,
  [ReflectionDataType.TIME]: PartitionNone,
  [ReflectionDataType.TIMESTAMP]: PartitionTimestamp,
  [ReflectionDataType.INTERVAL]: PartitionNone,
  [ReflectionDataType.STRUCT]: PartitionNone,
  [ReflectionDataType.LIST]: PartitionNone,
  [ReflectionDataType.MAP]: PartitionNone,
};
