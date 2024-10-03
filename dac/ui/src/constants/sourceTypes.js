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

// Note: These IDs need to match values in source.proto
export const REDSHIFT = "REDSHIFT";
export const S3 = "S3";
export const ELASTIC = "ELASTIC";
export const HBASE = "HBASE";
export const VASTDATA = "VASTDATA";
export const HDFS = "HDFS";
export const HIVE = "HIVE";
export const HIVE3 = "HIVE3";
export const MAPRFS = "MAPRFS";
export const SQLSERVER = "MSSQL";
export const MONGODB = "MONGO";
export const MYSQL = "MYSQL";
export const NAS = "NAS";
export const ORACLE = "ORACLE";
export const POSTGRESQL = "POSTGRES";
export const ADL = "ADL";
export const AWSGLUE = "AWSGLUE";
export const GCS = "GCS";
export const AMAZONELASTIC = "AMAZONELASTIC";
export const AZURE_STORAGE = "AZURE_STORAGE";
export const SYNAPSE = "SYNAPSE";
export const ADX = "ADX";
export const MSACCESS = "MSAccess";
export const SPARK = "SPARK";
export const SNOWFLAKE = "SNOWFLAKE";
export const DB2 = "DB2";
export const DREMIOTODREMIO = "DREMIOTODREMIO";
export const DRUID = "DRUID";
export const AZURE_SAMPLE_SOURCE = "SAMPLE_SOURCE";
export const HOME = "HOME";
export const INTERNAL = "INTERNAL";

// These are not implemented in the backend yet.
export const CASSANDRA = "CASSANDRA";
export const SALESFORCE = "SALESFORCE";
export const NETEZZA = "NETEZZA";
export const TERADATA = "TERADATA";

export const NESSIE = "NESSIE";
export const ARCTIC = "ARCTIC";
export const HISTORYTABLES = "HISTORYTABLES";

export const sourceProperties = [
  // todo: loc
  { label: "Amazon Redshift", sourceType: REDSHIFT, beta: true },
  { label: "Amazon S3", sourceType: S3, beta: true },
  { label: "Amazon OpenSearch Service", sourceType: AMAZONELASTIC },
  { label: "Elasticsearch", sourceType: ELASTIC },
  { label: "HBase", sourceType: HBASE, beta: true },
  { label: "VASTDATA", sourceType: VASTDATA, beta: true },
  { label: "HDFS", sourceType: HDFS },
  { label: "Hive 2.x", sourceType: HIVE },
  { label: "Hive 3.x", sourceType: HIVE3 },
  { label: "MapR-FS", sourceType: MAPRFS },
  { label: "Microsoft SQL Server", sourceType: SQLSERVER },
  { label: "MongoDB", sourceType: MONGODB, beta: true },
  { label: "MySQL", sourceType: MYSQL },
  { label: "NAS", sourceType: NAS },
  { label: "Oracle", sourceType: ORACLE },
  { label: "PostgreSQL", sourceType: POSTGRESQL },
  { label: "Azure Data Lake Store", sourceType: ADL, beta: true },
  { label: "AWS Glue Catalog", sourceType: AWSGLUE, beta: true },
  { label: "Google Cloud Storage", sourceType: GCS, beta: true },
  { label: "Microsoft Azure Synapse Analytics", sourceType: SYNAPSE },
  { label: "Microsoft Azure Data Explorer", sourceType: ADX },
  { label: "Snowflake", sourceType: SNOWFLAKE },
  { label: "Dremio to Dremio", sourceType: DREMIOTODREMIO },
  { label: "Druid", sourceType: DRUID },
  { label: "IBM Db2", sourceType: DB2 },
];

export const metastoresSourceType = {
  [HIVE]: true,
  [HIVE3]: true,
  [AWSGLUE]: true,
};

export const objectStorageSourceType = {
  [S3]: true,
  [ADL]: true,
  [AZURE_STORAGE]: true,
  [HDFS]: true,
  [MAPRFS]: true,
  [NAS]: true,
  [GCS]: true,
  [HISTORYTABLES]: true,
  [AZURE_SAMPLE_SOURCE]: true,
};

export const dataLakeSourceType = {
  [S3]: true,
  [ADL]: true,
  [AZURE_STORAGE]: true,
  [HDFS]: true,
  [MAPRFS]: true,
  [NAS]: true,
  [HIVE]: true,
  [HIVE3]: true,
  [AWSGLUE]: true,
  [GCS]: true,
  [HISTORYTABLES]: true,
};

const dataPlaneSources = {
  [NESSIE]: true,
  [ARCTIC]: true,
};

export const isDatabaseType = (sourceType) => {
  return (
    !dataPlaneSources[sourceType] &&
    !dataLakeSourceType[sourceType] &&
    sourceType !== AZURE_SAMPLE_SOURCE
  );
};

export const isDataLakeSourceType = (sourceType) => {
  return dataLakeSourceType[sourceType];
};

export const isDataPlaneSourceType = (sourceType) => {
  return dataPlaneSources[sourceType];
};

export const isMetastoreSourceType = (sourceType) => {
  return metastoresSourceType[sourceType];
};

export const isObjectStorageSourceType = (sourceType) => {
  return objectStorageSourceType[sourceType];
};

export const SHARING_TAB_JSON_TEMPLATE = {};

export const USE_LEGACY_DIALECT_PROPERTY_NAME = "useLegacyDialect";
