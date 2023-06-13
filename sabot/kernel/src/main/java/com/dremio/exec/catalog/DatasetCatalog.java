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
package com.dremio.exec.catalog;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Interface to perform actions on datasets.
 */
public interface DatasetCatalog extends PrivilegeCatalog {
  /**
   * Add or update a dataset.
   *
   * @param namespaceKey
   * @param dataset
   * @throws NamespaceException
   */
  void addOrUpdateDataset(NamespaceKey namespaceKey, DatasetConfig dataset) throws NamespaceException;

  CreateTableEntry createNewTable(NamespaceKey key, IcebergTableProps icebergTableProps,
                                  WriterOptions writerOptions, Map<String, Object> storageOptions);

  CreateTableEntry createNewTable(NamespaceKey key, IcebergTableProps icebergTableProps,
                                  WriterOptions writerOptions, Map<String, Object> storageOptions, boolean isResultsTable);

  void createEmptyTable(NamespaceKey key, BatchSchema batchSchema, WriterOptions writerOptions);

  void dropTable(NamespaceKey key, TableMutationOptions tableMutationOptions);

  void alterTable(NamespaceKey key, DatasetConfig datasetConfig, AlterTableOption alterTableOption, TableMutationOptions tableMutationOptions);

  void forgetTable(NamespaceKey key);

  /**
   * Create a new dataset at this location and mutate the dataset before saving.
   * @param key
   * @param datasetMutator
   */
  void createDataset(NamespaceKey key, com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator);

  UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions);
  UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions, boolean isPrivilegeValidationNeeded);

  /**
   * Create or update a physical dataset along with its read definitions and splits.
   *
   * @param userNamespaceService namespace service for a user who is adding or modifying a dataset.
   * @param source source where dataset is to be created/updated
   * @param datasetPath dataset full path
   * @param datasetConfig minimum configuration needed to define a dataset (format settings)
   * @param attributes optional namespace attributes
   * @return true if dataset is created/updated
   * @throws NamespaceException
   */
  boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException;

  /**
   * Update a dataset configuration with a newly detected schema.
   * @param datasetKey the dataset NamespaceKey
   * @param newSchema the detected schema from the executor
   */
  void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema);

  /**
   * Update a dataset configuration with a newly detected schema.
   * @param datasetKey the dataset NamespaceKey
   * @param originField the original field
   * @param fieldSchema the new schema
   */
  void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema);

  void truncateTable(NamespaceKey path, TableMutationOptions tableMutationOptions);

  void rollbackTable(NamespaceKey path, DatasetConfig datasetConfig, RollbackOption rollbackOption, TableMutationOptions tableMutationOptions);

  void addColumns(NamespaceKey datasetKey, DatasetConfig datasetConfig, List<Field> colsToAdd, TableMutationOptions tableMutationOptions);

  void dropColumn(NamespaceKey datasetKey, DatasetConfig datasetConfig, String columnToDrop, TableMutationOptions tableMutationOptions);

  void changeColumn(NamespaceKey datasetKey, DatasetConfig datasetConfig, String columnToChange, Field fieldFromSqlColDeclaration, TableMutationOptions tableMutationOptions);

  boolean alterDataset(final NamespaceKey key, final Map<String, AttributeValue> attributes);

  boolean alterColumnOption(final NamespaceKey key, String columnToChange,
                            final String attributeName, final AttributeValue attributeValue);

  void addPrimaryKey(final NamespaceKey namespaceKey, List<String> columns);

  void dropPrimaryKey(final NamespaceKey namespaceKey);

  List<String> getPrimaryKey(final NamespaceKey namespaceKey);

  boolean toggleSchemaLearning(NamespaceKey path, boolean enableSchemaLearning);

  Catalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping);

  Catalog resolveCatalogResetContext(String sourceName, VersionContext versionContext);

  /**
   * Retrieve a table
   *
   * @param datasetId
   * @return
   */
  DremioTable getTable(String datasetId);

  /**
   * Retrieve a table, first checking the default schema.
   *
   * @param key
   * @return
   */
  DremioTable getTable(NamespaceKey key);

  String getDatasetId(NamespaceKey key);

  enum UpdateStatus {
    /**
     * Metadata hasn't changed.
     */
    UNCHANGED,


    /**
     * Metadata has changed.
     */
    CHANGED,

    /**
     * Dataset has been deleted.
     */
    DELETED
  }
}
