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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.store.dfs.PhysicalDatasetUtils.toFileFormat;

import java.lang.reflect.UndeclaredThrowableException;
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
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.DatasetMetadataTooLargeException;
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
import com.google.common.base.Throwables;

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
  protected final DatasetConfig oldConfig;
  protected final int maxLeafColumns;

  private DatasetConfig datasetConfig;
  private ReadDefinition readDefinition;
  private List<DatasetSplit> splits;

  public FileSystemDatasetAccessor(
      FileSystemWrapper fs,
      FileSelection fileSelection,
      FileSystemPlugin fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      FormatPlugin formatPlugin,
      DatasetConfig oldConfig,
      int maxLeafColumns
  ) {
    this.updateKey = updateKey;
    this.fsPlugin =  fsPlugin;
    this.datasetPath = tableSchemaPath;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.formatPlugin = formatPlugin;
    this.oldConfig = oldConfig;
    this.maxLeafColumns = maxLeafColumns;
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
  public abstract BatchSchema getBatchSchema(FileSelection selection, FileSystemWrapper dfs) throws Exception;

  protected final DatasetConfig getDatasetInternal(
      FileSystemWrapper fs,
      FileSelection selection,
      List<String> tableSchemaPath
  ) {
    final UserGroupInformation processUGI = ImpersonationUtil.getProcessUserUGI();
    try {
      BatchSchema newSchema = processUGI.doAs(
          (PrivilegedExceptionAction<BatchSchema>) () -> {
            final Stopwatch watch = Stopwatch.createStarted();
            try {
              return getBatchSchema(selection, fs);
            } finally {
              logger.debug("Took {} ms to sample the schema of table located at: {}",
                  watch.elapsed(TimeUnit.MILLISECONDS), selection.getSelectionRoot());
            }
          }
      );
      DatasetType type = fs.isDirectory(new Path(selection.getSelectionRoot())) ?
          DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER :
          DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
      // Merge sampled schema into the existing one, if one already exists
      BatchSchema schema = newSchema;
      if (oldConfig != null && DatasetHelper.getSchemaBytes(oldConfig) != null) {
        schema = BatchSchema.fromDataset(oldConfig).merge(newSchema);
      }
      if (schema.getFieldCount() > maxLeafColumns) {
        throw new ColumnCountTooLargeException(
            String.format("Using datasets with more than %d columns is currently disabled.", maxLeafColumns));
      }
      return new DatasetConfig()
          .setName(tableSchemaPath.get(tableSchemaPath.size() - 1))
          .setType(type)
          .setFullPathList(tableSchemaPath)
          .setSchemaVersion(DatasetHelper.CURRENT_VERSION)
          .setRecordSchema(schema.toByteString())
          .setPhysicalDataset(new PhysicalDataset()
              .setFormatSettings(toFileFormat(formatPlugin).asFileConfig().setLocation(selection.getSelectionRoot())));
    } catch (DatasetMetadataTooLargeException e) {
      throw e;
    } catch (UndeclaredThrowableException e) {
      Throwables.throwIfInstanceOf(e.getUndeclaredThrowable(), DatasetMetadataTooLargeException.class);
      Throwables.throwIfUnchecked(e.getUndeclaredThrowable());
      throw new RuntimeException(e.getUndeclaredThrowable());
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
