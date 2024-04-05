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
package com.dremio.exec.store.easy;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.PartitionValue.PartitionValueType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyGroupScanUtils;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.util.MetadataSupportsInternalSchema;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetXAttr;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Schema;

/** Dataset accessor for text/json.. file formats */
public class EasyFormatDatasetAccessor
    implements FileDatasetHandle, MetadataSupportsInternalSchema {

  private final DatasetType type;
  private final FileSystem fs;
  private final FileSelection fileSelection;
  private final FileSystemPlugin<?> fsPlugin;
  private final NamespaceKey tableSchemaPath;
  private final FileUpdateKey updateKey;
  private final FormatPlugin formatPlugin;
  private final PreviousDatasetInfo oldConfig;
  private final int maxLeafColumns;
  private final boolean enableOptimalPartitionChunks;
  private final PartitionChunkListingImpl partitionChunkListing;

  private long recordCount;
  private EasyDatasetXAttr extended;
  private List<String> partitionColumns;

  public EasyFormatDatasetAccessor(
      DatasetType type,
      FileSystem fs,
      FileSelection fileSelection,
      FileSystemPlugin<?> fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      FormatPlugin formatPlugin,
      PreviousDatasetInfo oldConfig,
      int maxLeafColumns) {
    this.type = type;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.fsPlugin = fsPlugin;
    this.tableSchemaPath = tableSchemaPath;
    this.updateKey = updateKey;
    this.formatPlugin = formatPlugin;
    this.oldConfig = oldConfig;
    this.maxLeafColumns = maxLeafColumns;
    OptionResolver optionResolver = formatPlugin.getContext().getOptionManager();
    this.enableOptimalPartitionChunks =
        optionResolver.getOption(ExecConstants.ENABLE_OPTIMAL_FILE_PARTITION_CHUNKS);
    int maxSplitsPerChunk =
        this.enableOptimalPartitionChunks
            ? (int) optionResolver.getOption(ExecConstants.FILE_SPLITS_PER_PARTITION_CHUNK)
            : Integer.MAX_VALUE;
    this.partitionChunkListing = new PartitionChunkListingImpl(maxSplitsPerChunk);
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public FileConfigMetadata getDatasetMetadata(GetMetadataOption... options)
      throws ConnectorException {
    final BatchSchema schema;

    try {
      schema = getBatchSchema(oldConfig != null ? oldConfig.getSchema() : null, fileSelection, fs);
    } catch (Exception ex) {
      Throwables.propagateIfPossible(ex, ConnectorException.class);
      throw new ConnectorException(ex);
    }

    return new FileConfigMetadata() {
      @Override
      public DatasetStats getDatasetStats() {
        return DatasetStats.of(recordCount, ScanCostFactor.EASY.getFactor());
      }

      @Override
      public Schema getRecordSchema() {
        return schema;
      }

      @Override
      public List<String> getSortColumns() {
        if (oldConfig == null) {
          return Collections.emptyList();
        }

        return oldConfig.getSortColumns() == null
            ? Collections.emptyList()
            : oldConfig.getSortColumns();
      }

      @Override
      public List<String> getPartitionColumns() {
        return partitionColumns;
      }

      @Override
      public BytesOutput getExtraInfo() {
        return os -> extended.writeTo(os);
      }

      @Override
      public FileConfig getFileConfig() {
        return PhysicalDatasetUtils.toFileFormat(formatPlugin)
            .asFileConfig()
            .setLocation(fileSelection.getSelectionRoot());
      }
    };
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options)
      throws ConnectorException {
    try {
      buildIfNecessary();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }

    return partitionChunkListing;
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) {
    return updateKey::writeTo;
  }

  private BatchSchema getBatchSchema(
      BatchSchema oldSchema, final FileSelection selection, final FileSystem dfs) throws Exception {
    final SabotContext context = formatPlugin.getContext();
    try (BufferAllocator sampleAllocator =
            context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
        OperatorContextImpl operatorContext =
            new OperatorContextImpl(
                context.getConfig(),
                context.getDremioConfig(),
                sampleAllocator,
                context.getOptionManager(),
                1000,
                context.getExpressionSplitCache());
        SampleMutator mutator = new SampleMutator(sampleAllocator)) {
      final ImplicitFilesystemColumnFinder explorer =
          new ImplicitFilesystemColumnFinder(
              context.getOptionManager(),
              dfs,
              GroupScan.ALL_COLUMNS,
              ImplicitFilesystemColumnFinder.Mode.ALL_IMPLICIT_COLUMNS);

      Optional<FileAttributes> fileName =
          selection.getFileAttributesList().stream().filter(input -> input.size() > 0).findFirst();
      final FileAttributes file = fileName.orElse(selection.getFileAttributesList().get(0));

      EasyDatasetSplitXAttr dataset =
          EasyDatasetSplitXAttr.newBuilder()
              .setStart(0L)
              .setLength(Long.MAX_VALUE)
              .setPath(file.getPath().toString())
              .build();
      try (RecordReader reader =
          new AdditionalColumnsRecordReader(
              operatorContext,
              ((EasyFormatPlugin) formatPlugin)
                  .getRecordReader(operatorContext, dfs, dataset, GroupScan.ALL_COLUMNS),
              explorer.getImplicitFieldsForSample(selection),
              sampleAllocator)) {
        reader.setup(mutator);
        Map<String, ValueVector> fieldVectorMap = new HashMap<>();
        int i = 0;
        for (VectorWrapper<?> vw : mutator.getContainer()) {
          fieldVectorMap.put(vw.getField().getName(), vw.getValueVector());
          if (++i > maxLeafColumns) {
            throw new ColumnCountTooLargeException(maxLeafColumns);
          }
        }
        reader.allocate(fieldVectorMap);
        reader.next();
        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return getMergedSchema(
            oldSchema,
            mutator.getContainer().getSchema(),
            oldConfig.isSchemaLearningEnabled(),
            oldConfig.getDropColumns(),
            oldConfig.getModifiedColumns(),
            context.getOptionManager().getOption(ExecConstants.ENABLE_INTERNAL_SCHEMA),
            tableSchemaPath.getPathComponents(),
            file.getPath().toString());
      }
    }
  }

  private void buildIfNecessary() throws Exception {
    if (partitionChunkListing.computed()) {
      return;
    }
    final EasyGroupScanUtils easyGroupScanUtils =
        ((EasyFormatPlugin<?>) formatPlugin)
            .getGroupScan(SYSTEM_USERNAME, fsPlugin, fileSelection, GroupScan.ALL_COLUMNS);
    extended =
        EasyDatasetXAttr.newBuilder().setSelectionRoot(fileSelection.getSelectionRoot()).build();
    recordCount = easyGroupScanUtils.getScanStats().getRecordCount();

    // If we want to generate optimal partition chunks, only retrieve the partition implicit columns
    // to store as
    // partition values for partition chunks.  The legacy behavior retrieves all implicit columns -
    // such as file path
    // and mtime - as partition values, which results in single-split partition chunks in most
    // cases.
    ImplicitFilesystemColumnFinder.Mode mode =
        enableOptimalPartitionChunks
            ? ImplicitFilesystemColumnFinder.Mode.PARTITION_COLUMNS
            : ImplicitFilesystemColumnFinder.Mode.ALL_IMPLICIT_COLUMNS;
    final ImplicitFilesystemColumnFinder finder =
        new ImplicitFilesystemColumnFinder(
            fsPlugin.getContext().getOptionManager(), fs, GroupScan.ALL_COLUMNS, mode);
    final List<CompleteFileWork> work = easyGroupScanUtils.getChunks();
    final List<List<NameValuePair<?>>> pairs =
        finder.getImplicitFields(easyGroupScanUtils.getSelectionRoot(), work);
    final Set<String> allImplicitColumns = Sets.newLinkedHashSet();

    for (int i = 0; i < easyGroupScanUtils.getChunks().size(); i++) {
      final CompleteFileWork completeFileWork = work.get(i);

      final long size = completeFileWork.getTotalBytes();
      final String pathString = completeFileWork.getFileAttributes().getPath().toString();

      final List<DatasetSplitAffinity> affinities = new ArrayList<>();
      for (ObjectLongCursor<HostAndPort> item : completeFileWork.getByteMap()) {
        affinities.add(
            DatasetSplitAffinity.of(item.key.getHost(), completeFileWork.getTotalBytes()));
      }

      EasyDatasetSplitXAttr splitExtended =
          EasyDatasetSplitXAttr.newBuilder()
              .setPath(pathString)
              .setStart(completeFileWork.getStart())
              .setLength(completeFileWork.getLength())
              .setUpdateKey(
                  FileSystemCachedEntity.newBuilder()
                      .setPath(pathString)
                      .setLastModificationTime(
                          completeFileWork.getFileAttributes().lastModifiedTime().toMillis()))
              .build();

      List<PartitionValue> partitionValues = new ArrayList<>();

      // add implicit fields
      for (NameValuePair<?> p : pairs.get(i)) {
        Object obj = p.getValue();
        if (obj == null) {
          partitionValues.add(PartitionValue.of(p.getName(), PartitionValueType.IMPLICIT));
        } else if (obj instanceof String) {
          partitionValues.add(
              PartitionValue.of(p.getName(), (String) obj, PartitionValueType.IMPLICIT));
        } else if (obj instanceof Long) {
          partitionValues.add(
              PartitionValue.of(p.getName(), (long) obj, PartitionValueType.IMPLICIT));
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Unable to handle value %s of type %s.", obj, obj.getClass().getName()));
        }
        allImplicitColumns.add(p.getName());
      }
      long splitRecordCount = 0; // unknown.
      DatasetSplit split =
          DatasetSplit.of(affinities, size, splitRecordCount, splitExtended::writeTo);
      partitionChunkListing.put(partitionValues, split);
    }
    partitionColumns = ImmutableList.copyOf(allImplicitColumns);
    partitionChunkListing.computePartitionChunks();
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }
}
