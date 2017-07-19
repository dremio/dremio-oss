/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.store.dfs.PhysicalDatasetUtils.toFileFormat;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Stopwatch;

/**
 * Dataset accessor for filesystem based dataset.
 */
public abstract class FileSystemDatasetAccessor implements SourceTableDefinition {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemDatasetAccessor.class);

  public static final Serializer<FileUpdateKey> FILE_UPDATE_KEY_SERIALIZER = ProtostuffSerializer.of(FileUpdateKey.getSchema());

  protected final NamespaceKey datasetPath;
  protected final FileSystemPlugin fsPlugin;
  protected final FileSystemWrapper fs;
  protected final FileSelection fileSelection;
  protected final FormatPlugin formatPlugin;
  protected final FileUpdateKey updateKey;

  private DatasetConfig datasetConfig;
  private ReadDefinition readDefinition;
  private String tableName;
  private List<DatasetSplit> splits;

  public FileSystemDatasetAccessor(FileSystemWrapper fs,
                                   FileSelection fileSelection,
                                   FileSystemPlugin fsPlugin,
                                   NamespaceKey tableSchemaPath,
                                   String tableName,
                                   FileUpdateKey updateKey,
                                   FormatPlugin formatPlugin) {
    this.tableName = tableName;
    this.updateKey = updateKey;
    this.fsPlugin =  fsPlugin;
    this.datasetPath = tableSchemaPath;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.formatPlugin = formatPlugin;
  }

  @Override
  public NamespaceKey getName() {
    return datasetPath;
  }

  public NamespaceKey getDatasetPath() {
    return datasetPath;
  }

  public FileSystemPlugin getFsPlugin() {
    return fsPlugin;
  }

  public FileSystemWrapper getFs() {
    return fs;
  }

  public FileSelection getFileSelection() {
    return fileSelection;
  }

  public ReadDefinition getMetaData() {
    return readDefinition;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    if (datasetConfig != null) {
      return datasetConfig;
    }
    datasetConfig = buildDataset();
    readDefinition = buildMetadata();
    datasetConfig.setReadDefinition(readDefinition);
    if(datasetConfig.getId() == null){
      datasetConfig.setId(new EntityId(UUID.randomUUID().toString()));
    }
    return datasetConfig;
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    if (splits != null) {
      return splits;
    }
    splits = new ArrayList<>(buildSplits());
    return splits;
  }

  public abstract Collection<DatasetSplit> buildSplits() throws Exception;
  public abstract ReadDefinition buildMetadata() throws Exception;
  public abstract DatasetConfig buildDataset() throws Exception;
  public abstract BatchSchema getBatchSchema(FileSelection selection, FileSystemWrapper dfs);

  protected DatasetConfig getDatasetInternal(final FileSystemWrapper fs, final FileSelection selection, List<String> tableSchemaPath) {
    final UserGroupInformation processUGI = ImpersonationUtil.getProcessUserUGI();
    BatchSchema schema = null;
    try {
      schema = processUGI.doAs(
        new PrivilegedExceptionAction<BatchSchema>() {
          @Override
          public BatchSchema run() throws Exception {
            final Stopwatch watch = Stopwatch.createStarted();
            try {
              return getBatchSchema(selection, fs);
            } finally {
              logger.debug("Took {} ms to sample the schema of table located at: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), selection.selectionRoot);
            }
          }
        }
      );
      DatasetType type = fs.isDirectory(new Path(selection.getSelectionRoot())) ?
        DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER :
        DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
      return new DatasetConfig()
        .setName(tableSchemaPath.get(tableSchemaPath.size() - 1))
        .setType(type)
        .setFullPathList(tableSchemaPath)
        .setSchemaVersion(DatasetHelper.CURRENT_VERSION)
        .setRecordSchema(schema.toByteString())
        .setPhysicalDataset(new PhysicalDataset()
          .setFormatSettings(toFileFormat(formatPlugin).asFileConfig().setLocation(selection.getSelectionRoot())));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
