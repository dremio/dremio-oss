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
package com.dremio.exec.store.hive.metadata;

import java.util.Collections;
import java.util.Iterator;

import org.apache.thrift.TException;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.store.hive.HiveClient;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

/**
 * Produces of DatasetHandle instances lazily as the caller uses the iterator.
 */
public class HiveDatasetHandleListing implements DatasetHandleListing {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDatasetHandleListing.class);

  private final HiveClient client;
  private final boolean ignoreAuthzErrors;
  private final String pluginName;

  private final Iterator<String> dbNames;

  private String currentDbName;
  private Iterator<String> tableNamesInCurrentDb;

  public HiveDatasetHandleListing(final HiveClient client, final String pluginName,
                                  final boolean ignoreAuthzErrors) {

    this.client = client;
    this.ignoreAuthzErrors = ignoreAuthzErrors;
    this.pluginName = pluginName;

    dbNames = getDatabaseNames();
    advanceToNonEmptyDatabase();
  }

  @Override
  public Iterator<DatasetHandle> iterator() {
    return new DatasetHandleIterator();
  }

  private class DatasetHandleIterator extends AbstractIterator<DatasetHandle> {
    @Override
    protected DatasetHandle computeNext() {

      if (!tableNamesInCurrentDb.hasNext()) {
        advanceToNonEmptyDatabase();

        if (!dbNames.hasNext() && !tableNamesInCurrentDb.hasNext()) {
          logger.debug("Plugin '{}', has no more tables.", pluginName, currentDbName);
          return endOfData();
        }
      }

      if (tableNamesInCurrentDb.hasNext()) {
        String tableName = tableNamesInCurrentDb.next();

        logger.debug("Plugin '{}', database '{}', table '{}', Table found.", pluginName, currentDbName, tableName);

        EntityPath datasetPath = new EntityPath(ImmutableList.of(pluginName, currentDbName, tableName));

        return HiveDatasetHandle
          .newBuilder()
          .datasetpath(datasetPath)
          .build();
      } else {
        logger.debug("Plugin '{}', has no more tables.", pluginName, currentDbName);
        return endOfData();
      }
    }
  }

  private void advanceToNonEmptyDatabase() {
    do {
      if (dbNames.hasNext()) {
        currentDbName = dbNames.next();
        tableNamesInCurrentDb = getTableNames(currentDbName);

        logger.debug("Plugin '{}', database '{}', Database found.", pluginName, currentDbName);
      }
    } while (!tableNamesInCurrentDb.hasNext() && dbNames.hasNext());
  }

  private Iterator<String> getDatabaseNames() {
    try {
      return client.getDatabases(ignoreAuthzErrors).iterator();
    } catch (TException e) {
      logger.warn("Plugin '{}', Unable to retrieve database names.", pluginName, e);
      return Collections.emptyIterator();
    }
  }

  private Iterator<String> getTableNames(String dbName) {
    try {
      return client.getTableNames(dbName, ignoreAuthzErrors).iterator();
    } catch (TException e) {
      logger.warn("Plugin '{}', database '{}', Unable to retrieve table names.", pluginName, dbName, e);
      return Collections.emptyIterator();
    }
  }
}
