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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.lang.String.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.MinorType;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemDatasetAccessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.MetadataUtils;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.parquet.ParquetGroupScanUtils.RowGroupInfo;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionValueType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.ColumnValueCount;
import com.dremio.service.namespace.file.proto.DictionaryEncodedColumns;
import com.dremio.service.namespace.file.proto.FileSystemCachedEntity;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.ParquetDatasetXAttr;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Parquet dataset accessor.
 * ReadDefinition and splits are computed as same time as dataset.
 */
public class ParquetFormatDatasetAccessor extends FileSystemDatasetAccessor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFormatDatasetAccessor.class);

  private final DatasetConfig oldConfig;
  private ReadDefinition cachedParquetReadDefinition;
  private List<DatasetSplit> cachedParquetSplits;
  private boolean builtAll = false;

  public ParquetFormatDatasetAccessor(DatasetConfig oldConfig, FileSystemWrapper fs, FileSelection fileSelection, FileSystemPlugin fsPlugin,
                                      NamespaceKey tableSchemaPath, String tableName, FileUpdateKey updateKey,
                                      ParquetFormatPlugin formatPlugin) {
    super(fs, fileSelection, fsPlugin, tableSchemaPath, tableName, updateKey, formatPlugin, oldConfig);
    this.oldConfig = oldConfig;
  }

  @Override
  public Collection<DatasetSplit> buildSplits() throws Exception {
    if (!builtAll) {
      buildDataset();
    }
    return cachedParquetSplits;
  }

  @Override
  public ReadDefinition buildMetadata() throws Exception {
    if (!builtAll) {
      buildDataset();
    }
    return cachedParquetReadDefinition;
  }

  @Override
  public BatchSchema getBatchSchema(final FileSelection selection, final FileSystemWrapper fs) {
    final SabotContext context = ((ParquetFormatPlugin)formatPlugin).getContext();
    try (
      BufferAllocator sampleAllocator = context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
      OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), sampleAllocator, context.getOptionManager(), 1000);
      SampleMutator mutator = new SampleMutator(context)
    ){
      final Optional<FileStatus> firstFileO = selection.getFirstFile();
      if(!firstFileO.isPresent()) {
        throw UserException.dataReadError().message("Unable to find any files for datasets.").build(logger);
      }
      final FileStatus firstFile = firstFileO.get();
      final ParquetMetadata footer = ParquetFileReader.readFooter(fsPlugin.getFsConf(), firstFile, ParquetMetadataConverter.NO_FILTER);
      final ParquetReaderUtility.DateCorruptionStatus dateStatus = ParquetReaderUtility.detectCorruptDates(footer, GroupScan.ALL_COLUMNS,
        ((ParquetFormatPlugin)formatPlugin).getConfig().autoCorrectCorruptDates);
      final boolean readInt96AsTimeStamp = operatorContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).bool_val;
      final ImplicitFilesystemColumnFinder finder = new  ImplicitFilesystemColumnFinder(context.getOptionManager(), fs, GroupScan.ALL_COLUMNS);

      try(RecordReader reader =
            new AdditionalColumnsRecordReader(
              new ParquetRowiseReader(operatorContext, footer, 0, firstFile.getPath().toString(), GroupScan.ALL_COLUMNS, fs, dateStatus, readInt96AsTimeStamp, true),
              finder.getImplicitFieldsForSample(selection)
            )) {

        reader.setup(mutator);

        mutator.allocate(100);
        //TODO DX-3873: remove the next() call here. We need this for now since we don't populate inner list types until next.
        reader.next();

        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return mutator.getContainer().getSchema();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DatasetConfig buildDataset() throws Exception {
    final DatasetConfig datasetConfig = super.getDatasetInternal(fs, fileSelection, datasetPath.getPathComponents());
    buildAll(datasetConfig);
    return datasetConfig;
  }


  private void buildAll(DatasetConfig datasetConfig) throws Exception {
    final BatchSchema schema = BatchSchema.fromDataset(datasetConfig);
    final ParquetGroupScanUtils parquetGroupScanUtils = ((ParquetFormatPlugin) formatPlugin).getGroupScan(SYSTEM_USERNAME,
      fsPlugin, fileSelection, datasetConfig.getFullPathList(), GroupScan.ALL_COLUMNS, schema, null);
    this.cachedParquetReadDefinition = new ReadDefinition();

    // write down old sort columns if there were any.
    if(oldConfig != null && oldConfig.getReadDefinition() != null){
      cachedParquetReadDefinition.setSortColumnsList(oldConfig.getReadDefinition().getSortColumnsList());
    }
    cachedParquetReadDefinition.setScanStats(MetadataUtils.fromPojoScanStats(parquetGroupScanUtils.getScanStats()).setScanFactor(ScanCostFactor.PARQUET.getFactor()));
    cachedParquetReadDefinition.setReadSignature(ByteString.copyFrom(FileSystemDatasetAccessor.FILE_UPDATE_KEY_SERIALIZER.serialize(updateKey)));

    // compute splits
    this.cachedParquetSplits = getSplits(parquetGroupScanUtils, cachedParquetReadDefinition);

    // scan for global dictionaries
    final DictionaryEncodedColumns dictionaryEncodedColumns =  ParquetFormatPlugin.scanForDictionaryEncodedColumns(fs, fileSelection.getSelectionRoot(), schema);
    if (dictionaryEncodedColumns != null) {
      logger.debug("Found global dictionaries for table {} for columns {}", datasetPath, dictionaryEncodedColumns);
    }
    final List<ColumnValueCount> columnValueCounts = Lists.newArrayList();

    for (Map.Entry<SchemaPath, Long> entry : parquetGroupScanUtils.getColumnValueCounts().entrySet()) {
      columnValueCounts.add(new ColumnValueCount()
        .setColumn(entry.getKey().getAsUnescapedPath())
        .setCount(entry.getValue()));
    }

    cachedParquetReadDefinition.setExtendedProperty(ByteString.copyFrom(ParquetDatasetXAttrSerDe.PARQUET_DATASET_XATTR_SERIALIZER.serialize(
      new ParquetDatasetXAttr()
      .setSelectionRoot(fileSelection.getSelectionRoot())
      .setColumnValueCountsList(columnValueCounts)
      .setDictionaryEncodedColumns(dictionaryEncodedColumns))));

    builtAll = true;
  }

  private List<DatasetSplit> getSplits(ParquetGroupScanUtils parquetGroupScanUtils, ReadDefinition readDefinition) throws IOException {
    final List<DatasetSplit> splits = Lists.newArrayList();

    final ImplicitFilesystemColumnFinder finder = new  ImplicitFilesystemColumnFinder(getFsPlugin().getContext().getOptionManager(), fs, GroupScan.ALL_COLUMNS);
    List<RowGroupInfo> rowGroups = parquetGroupScanUtils.getRowGroupInfos();

    final List<List<NameValuePair<?>>> pairs = finder.getImplicitFields(parquetGroupScanUtils.getSelectionRoot(), rowGroups);
    final Set<String> allImplicitColumns = Sets.newLinkedHashSet();

    for(int i =0; i < parquetGroupScanUtils.getRowGroupInfos().size(); i++){
      final ParquetGroupScanUtils.RowGroupInfo rowGroupInfo = parquetGroupScanUtils.getRowGroupInfos().get(i);
      final DatasetSplit split = new DatasetSplit();
      final String pathString = rowGroupInfo.getStatus().getPath().toString();

      split.setRowCount(rowGroupInfo.getRowCount());
      split.setSize(rowGroupInfo.getTotalBytes());
      split.setSplitKey(format("%s:[%d-%d]", pathString, rowGroupInfo.getStart(), rowGroupInfo.getLength()));

      // set affinity
      final List<Affinity> affinities = Lists.newArrayList();
      final Iterator<ObjectLongCursor<CoordinationProtos.NodeEndpoint>> nodeEndpointIterator = rowGroupInfo.getByteMap().iterator();
      while (nodeEndpointIterator.hasNext()) {
        ObjectLongCursor<CoordinationProtos.NodeEndpoint> nodeEndpointObjectLongCursor = nodeEndpointIterator.next();
        CoordinationProtos.NodeEndpoint endpoint = nodeEndpointObjectLongCursor.key;
        affinities.add(new Affinity().setHost(endpoint.getAddress()).setFactor((double)nodeEndpointObjectLongCursor.value));
      }
      split.setAffinitiesList(affinities);

      // set partition column values
      final LinkedHashMap<String, PartitionValue> partitionValues = new LinkedHashMap<>();
      // get single valued column partition
      for (Map.Entry<SchemaPath, Object> entry: parquetGroupScanUtils.getPartitionValueMap().get(rowGroupInfo.getStatus()).entrySet()) {
        final SchemaPath columnSchemaPath = entry.getKey();
        Preconditions.checkNotNull(parquetGroupScanUtils.getColumnTypeMap());
        MajorType type = parquetGroupScanUtils.getColumnTypeMap().get(columnSchemaPath);
        if(type != null){
          final MinorType minorType = MinorType.valueOf(parquetGroupScanUtils.getColumnTypeMap().get(columnSchemaPath).getMinorType().getNumber());
          partitionValues.put(columnSchemaPath.getAsUnescapedPath(), MetadataUtils.toPartitionValue(columnSchemaPath, entry.getValue(), minorType).setType(PartitionValueType.VISIBLE));
        }
      }

      // add implicit fields
      for(NameValuePair<?> p : pairs.get(i)) {
        if (!partitionValues.containsKey(p.getName())) {
          final Object obj = p.getValue();
          PartitionValue value;
          if(obj == null || obj instanceof String){
            value = new PartitionValue().setColumn(p.getName()).setStringValue( (String) p.getValue()).setType(PartitionValueType.IMPLICIT);
          }else if(obj instanceof Long){
            value = new PartitionValue().setColumn(p.getName()).setLongValue((Long) p.getValue()).setType(PartitionValueType.IMPLICIT);
          }else{
            throw new UnsupportedOperationException(String.format("Unable to handle value %s of type %s.", obj, obj.getClass().getName()));

          }
          partitionValues.put(p.getName(), value);
          allImplicitColumns.add(p.getName());
        }
      }
      split.setPartitionValuesList(new ArrayList<>(partitionValues.values()));

      List<ColumnValueCount> columnValueCounts = Lists.newArrayList();
      for (Map.Entry<SchemaPath, Long> entry: rowGroupInfo.getColumnValueCounts().entrySet()) {
        columnValueCounts.add(new ColumnValueCount()
          .setColumn(entry.getKey().getAsUnescapedPath())
          .setCount(entry.getValue()));
      }

      // set xattr
      split.setExtendedProperty(ByteString.copyFrom(ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.serialize(
        new ParquetDatasetSplitXAttr()
          .setPath(pathString)
          .setStart(rowGroupInfo.getStart())
          .setRowGroupIndex(rowGroupInfo.getRowGroupIndex())
          .setUpdateKey(new FileSystemCachedEntity()
              .setPath(pathString)
              .setLastModificationTime(rowGroupInfo.getStatus().getModificationTime()))
          .setColumnValueCountsList(columnValueCounts)
          .setLength(rowGroupInfo.getLength()))));

      splits.add(split);
    }

    final List<String> filePartitionColumns = MetadataUtils.getStringColumnNames(parquetGroupScanUtils.getPartitionColumns());
    if (filePartitionColumns != null) {
      allImplicitColumns.addAll(filePartitionColumns);
    }
    readDefinition.setPartitionColumnsList(Lists.newArrayList(allImplicitColumns));
    return splits;
  }

  @Override
  public boolean isSaveable() {
    return true;
  }

  @Override
  public DatasetType getType() {
    // TODO: update to correctly detect file verus folder type.
    return DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
  }
}
