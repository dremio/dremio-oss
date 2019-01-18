/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
export const REDSHIFT = 'REDSHIFT';
export const S3 = 'S3';
export const ELASTIC = 'ELASTIC';
export const HBASE = 'HBASE';
export const HDFS = 'HDFS';
export const HIVE = 'HIVE';
export const DB2 = 'DB2';
export const MAPRFS = 'MAPRFS';
export const SQLSERVER = 'MSSQL';
export const MONGODB = 'MONGO';
export const MYSQL = 'MYSQL';
export const NAS = 'NAS';
export const ORACLE = 'ORACLE';
export const POSTGRESQL = 'POSTGRES';
export const ADL = 'ADL';

// These are not implemented in the backend yet.
export const CASSANDRA = 'CASSANDRA';
export const SALESFORCE = 'SALESFORCE';
export const NETEZZA = 'NETEZZA';
export const TERADATA = 'TERADATA';

export const sourceProperties = [
  // todo: loc
  {label: 'Amazon Redshift', sourceType: REDSHIFT, beta: true},
  {label: 'Amazon S3', sourceType: S3, beta: true},
  {label: 'Elasticsearch', sourceType: ELASTIC},
  {label: 'HBase', sourceType: HBASE, beta: true},
  {label: 'HDFS', sourceType: HDFS},
  {label: 'Hive', sourceType: HIVE},
  {label: 'MapR-FS', sourceType: MAPRFS},
  {label: 'Microsoft SQL Server', sourceType: SQLSERVER},
  {label: 'MongoDB', sourceType: MONGODB, beta: true},
  {label: 'MySQL', sourceType: MYSQL},
  {label: 'NAS', sourceType: NAS},
  {label: 'Oracle', sourceType: ORACLE},
  {label: 'PostgreSQL', sourceType: POSTGRESQL},
  {label: 'Azure Data Lake Store', sourceType: ADL, beta: true}
];

export const SHARING_TAB_JSON_TEMPLATE = {};
