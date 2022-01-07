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
package com.dremio.exec.store.hive;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

import com.dremio.hive.thrift.TException;

/**
 * Wrapper around HiveMetaStoreClient to provide additional capabilities such as caching, reconnecting with user
 * credentials and higher level APIs to get the metadata in form that Dremio needs directly.
 */
public interface HiveClient extends AutoCloseable {
  List<String> getDatabases(boolean ignoreAuthzErrors) throws TException;

  boolean databaseExists(final String dbName);

  List<String> getTableNames(String dbName, boolean ignoreAuthzErrors) throws TException;

  boolean tableExists(final String dbName, final String tableName) throws TException ;

  Table getTable(String dbName, String tableName, boolean ignoreAuthzErrors) throws TException;

  List<Partition> getPartitionsByName(String dbName, String tableName, List<String> partitionNames) throws TException;

  List<String> getPartitionNames(String dbName, String tableName) throws TException;

  String getDelegationToken(final String proxyUser) throws TException;

  List<HivePrivilegeObject> getRowFilterAndColumnMasking(List<HivePrivilegeObject> inputHiveObjects) throws
    SemanticException;

  @Override
  void close();
}
