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
package com.dremio.exec.store.easy;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.lang.String.format;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.fs.Path;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemDatasetAccessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.MetadataUtils;
import com.dremio.exec.store.dfs.easy.EasyDatasetXAttrSerDe;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyGroupScanUtils;
import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionValueType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.EasyDatasetXAttr;
import com.dremio.service.namespace.file.proto.FileSystemCachedEntity;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Dataset accessor for text/avro/json.. file formats
 */
public class EasyFormatDatasetAccessor extends FileSystemDatasetAccessor {

  private ReadDefinition cachedMetadata;
  private List<DatasetSplit> cachedSplits;
  private boolean builtAll = false;

  public EasyFormatDatasetAccessor(FileSystemWrapper fs, FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, String tableName, FileUpdateKey updateKey, FormatPlugin formatPlugin) {
    super(fs, fileSelection, fsPlugin, tableSchemaPath, tableName, updateKey, formatPlugin);
  }

  @Override
  public Collection<DatasetSplit> buildSplits() throws Exception {
    if (!builtAll) {
      buildDataset();
    }
    return cachedSplits;
  }

  @Override
  public ReadDefinition buildMetadata() throws Exception {
    if (!builtAll) {
      buildDataset();
    }
    return cachedMetadata;
  }

  @Override
  public DatasetConfig buildDataset() throws Exception {
    final DatasetConfig datasetConfig = getDatasetInternal(fs, fileSelection, datasetPath.getPathComponents());
    buildAll(datasetConfig);
    return datasetConfig;
  }

  @Override
  public BatchSchema getBatchSchema(final FileSelection selection, final FileSystemWrapper dfs) {
    final SabotContext context = formatPlugin.getContext();
    try (
      BufferAllocator sampleAllocator = context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
      OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), sampleAllocator, context.getOptionManager(), 1000);
      SampleMutator mutator = new SampleMutator(context)
    ){
      final ImplicitFilesystemColumnFinder explorer = new ImplicitFilesystemColumnFinder(context.getOptionManager(), dfs, GroupScan.ALL_COLUMNS);
      String file;
      Optional<String> fileName = Iterables.tryFind(selection.getFiles(), new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String input) {
          try {
            return dfs.getFileStatus(new Path(input)).getLen() > 0;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      if (fileName.isPresent()) {
        file = fileName.get();
      } else {
        file = selection.getFiles().get(0);
      }
      final FileWork fileWork = new CompleteFileWork.FileWorkImpl(0, Long.MAX_VALUE, file);
      try(RecordReader reader = new AdditionalColumnsRecordReader(((EasyFormatPlugin)formatPlugin).getRecordReader(operatorContext, dfs, fileWork, GroupScan.ALL_COLUMNS), explorer.getImplicitFields(selection))) {
        reader.setup(mutator);
        Map<String, ValueVector> fieldVectorMap = new HashMap<>();
        for (VectorWrapper<?> vw : mutator.getContainer()) {
          fieldVectorMap.put(vw.getField().getName(), vw.getValueVector());
        }
        reader.allocate(fieldVectorMap);
        reader.next();
        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return mutator.getContainer().getSchema();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void buildAll(DatasetConfig datasetConfig) throws Exception {
    final EasyGroupScanUtils easyGroupScanUtils = ((EasyFormatPlugin) formatPlugin).getGroupScan(SYSTEM_USERNAME, fsPlugin, fileSelection, GroupScan.ALL_COLUMNS);

    cachedMetadata = new ReadDefinition()
      .setLastRefreshDate(System.currentTimeMillis())
      .setScanStats(MetadataUtils.fromPojoScanStats(easyGroupScanUtils.getScanStats()).setScanFactor(ScanCostFactor.EASY.getFactor()))
      .setReadSignature(ByteString.copyFrom(FILE_UPDATE_KEY_SERIALIZER.serialize(updateKey)))
      .setPartitionColumnsList(MetadataUtils.getStringColumnNames(easyGroupScanUtils.getPartitionColumns()))
      .setExtendedProperty(
          ByteString.copyFrom(EasyDatasetXAttrSerDe.EASY_DATASET_XATTR_SERIALIZER.serialize(
        new EasyDatasetXAttr().setSelectionRoot(fileSelection.getSelectionRoot()))));

    //cachedMetadata.setSortColumnsList(easyGroupScanUtils.getSortColumns()); // TODO(AH) probably not needed since they are set in layout info?

    // compute splits
    this.cachedSplits = getSplits(datasetConfig, easyGroupScanUtils);
    this.builtAll = true;
  }

  private List<DatasetSplit> getSplits(DatasetConfig datasetConfig, EasyGroupScanUtils easyGroupScanUtils) throws IOException {
    final List<DatasetSplit> splits = Lists.newArrayList();

    final ImplicitFilesystemColumnFinder finder = new  ImplicitFilesystemColumnFinder(getFsPlugin().getContext().getOptionManager(), fs, GroupScan.ALL_COLUMNS);
    final List<String> paths = FluentIterable.from(easyGroupScanUtils.getChunks()).transform(new Function<CompleteFileWork, String>(){
      @Override
      public String apply(CompleteFileWork input) {
        return input.getPath();
      }}).toList();

    final List<List<NameValuePair<?>>> pairs = finder.getImplicitFields(easyGroupScanUtils.getSelectionRoot(), paths);
    final Set<String> allImplicitColumns = Sets.newLinkedHashSet();

    for(int i =0; i < easyGroupScanUtils.getChunks().size(); i++){
      final CompleteFileWork completeFileWork = easyGroupScanUtils.getChunks().get(i);
      final DatasetSplit split = new DatasetSplit();
      split.setSize(completeFileWork.getTotalBytes());
      split.setSplitKey(format("%s:[%d-%d]", completeFileWork.getPath(), completeFileWork.getStart(), completeFileWork.getLength()));
      final List<Affinity> affinities = Lists.newArrayList();
      final Iterator<ObjectLongCursor<CoordinationProtos.NodeEndpoint>> nodeEndpointIterator = completeFileWork.getByteMap().iterator();
      while (nodeEndpointIterator.hasNext()) {
        CoordinationProtos.NodeEndpoint endpoint = nodeEndpointIterator.next().key;
        affinities.add(new Affinity().setHost(endpoint.getAddress()).setFactor((double) completeFileWork.getTotalBytes()));
      }
      split.setAffinitiesList(affinities);

      split.setExtendedProperty(ByteString.copyFrom(EasyDatasetXAttrSerDe.EASY_DATASET_SPLIT_XATTR_SERIALIZER.serialize(
        new EasyDatasetSplitXAttr()
        .setPath(completeFileWork.getPath())
        .setStart(completeFileWork.getStart())
        .setLength(completeFileWork.getLength())
        .setUpdateKey(new FileSystemCachedEntity()
            .setPath(completeFileWork.getPath())
            .setLastModificationTime(easyGroupScanUtils.getFileStatus(completeFileWork.getPath()).getModificationTime()))
        )));

      final List<PartitionValue> partitionValues = Lists.newArrayList();
      // add implicit fields
      for(NameValuePair<?> p : pairs.get(i)) {
        Object obj = p.getValue();
        if(obj == null || obj instanceof String){
          partitionValues.add(new PartitionValue().setColumn(p.getName()).setStringValue( (String) obj).setType(PartitionValueType.IMPLICIT));
        }else if(obj instanceof Long){
          partitionValues.add(new PartitionValue().setColumn(p.getName()).setLongValue((Long) obj).setType(PartitionValueType.IMPLICIT));
        }else{
          throw new UnsupportedOperationException(String.format("Unable to handle value %s of type %s.", obj, obj.getClass().getName()));
        }
        allImplicitColumns.add(p.getName());
      }
      split.setPartitionValuesList(partitionValues);

      splits.add(split);
    }
    cachedMetadata.setPartitionColumnsList(Lists.newArrayList(allImplicitColumns));
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
