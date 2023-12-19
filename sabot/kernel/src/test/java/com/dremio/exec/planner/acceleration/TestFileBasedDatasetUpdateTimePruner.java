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

package com.dremio.exec.planner.acceleration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.planner.cost.DremioRelMetadataQuery;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.options.OptionManager;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.AbstractPartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.DremioTest;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

public class TestFileBasedDatasetUpdateTimePruner extends DremioTest {

  private final Map<String, List<Split>> SPLITS = ImmutableMap.<String, List<Split>>builder()
      .put("A", ImmutableList.of(
          Split.of("file1", 1),
          Split.of("file2", 5),
          Split.of("file3", 9)))
      .put("B", ImmutableList.of(
          Split.of("file4", 2),
          Split.of("file5", 5),
          Split.of("file6", 8)))
      .put("C", ImmutableList.of(
          Split.of("file7", 5),
          Split.of("file8", 10)))
      .build();

  private RelOptCluster cluster;

  @Before
  public void setup() {
    OptionValidatorListingImpl validatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    OptionManager options = OptionManagerWrapper.Builder.newBuilder()
        .withOptionManager(new DefaultOptionManager(validatorListing))
        .withOptionManager(new SessionOptionManagerImpl(validatorListing))
        .build();

    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);
    PlannerSettings plannerSettings =
        new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, options, () -> info);
    cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings),  new RexBuilder(JavaTypeFactoryImpl.INSTANCE));
    cluster.setMetadataQuery(DremioRelMetadataQuery.QUERY_SUPPLIER);
  }

  @Test
  public void testPruning() {
    validatePartitionedPruning(SPLITS, 0);
    validatePartitionedPruning(SPLITS, 1);
    validatePartitionedPruning(SPLITS, 5);
    validatePartitionedPruning(SPLITS, 9);
    validatePartitionedPruning(SPLITS, 10);
  }

  private void validatePartitionedPruning(Map<String, List<Split>> splits, long minModTime) {
    ScanCrel scan = createScan(FileType.JSON, splits);
    ScanCrel prunedScan = (ScanCrel) FileBasedDatasetUpdateTimePruner.prune(scan, minModTime);

    Set<String> remainingPartitions = new HashSet<>(splits.keySet());
    for (Iterator<PartitionChunkMetadata> it = prunedScan.getTableMetadata().getSplits(); it.hasNext(); ) {
      PartitionChunkMetadata chunk = it.next();

      PartitionProtobuf.PartitionValue partitionValue = chunk.getPartitionValues().iterator().next();
      remainingPartitions.remove(partitionValue.getStringValue());
      List<Split> expectedSplits = splits.get(partitionValue.getStringValue()).stream()
          .filter(split -> split.getLastModifiedTime() > minModTime)
          .collect(Collectors.toList());
      List<Split> actualSplits = getActualSplits(chunk);
      assertThat(actualSplits).containsExactlyInAnyOrderElementsOf(expectedSplits);
    }

    for (String partitionValue : remainingPartitions) {
      List<Split> expectedSplits = splits.get(partitionValue).stream()
          .filter(split -> split.getLastModifiedTime() > minModTime)
          .collect(Collectors.toList());
      assertThat(expectedSplits).hasSize(0);
    }
  }

  private List<Split> getActualSplits(PartitionChunkMetadata chunk) {
    return Streams.stream(chunk.getDatasetSplits())
        .map(split -> {
          try {
            EasyProtobuf.EasyDatasetSplitXAttr splitXAttr =
                EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(split.getSplitExtendedProperty());
            return Split.of(splitXAttr.getPath(), splitXAttr.getUpdateKey().getLastModificationTime());
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        })
        .collect(Collectors.toList());
  }

  private ScanCrel createScan(FileType fileType, Map<String, List<Split>> splits) {
    return new ScanCrel(cluster, RelTraitSet.createEmpty(), mock(StoragePluginId.class),
        getTableMetadata(fileType, splits), ImmutableList.of(),  0, ImmutableList.of(),
        false, false);
  }

  private TableMetadata getTableMetadata(FileType fileType, Map<String, List<Split>> splits) {
    FileConfig fileConfig = new FileConfig()
        .setType(fileType);

    PhysicalDataset physicalDataset = new PhysicalDataset()
        .setFormatSettings(fileConfig);

    ReadDefinition readDefinition = new ReadDefinition();

    DatasetConfig datasetConfig = new DatasetConfig()
        .setPhysicalDataset(physicalDataset)
        .setReadDefinition(readDefinition)
        .setRecordSchema(BatchSchema.EMPTY.toByteString());

    SplitsPointer splitsPointer = getSplitsPointer(splits);
    return new TableMetadataImpl(mock(StoragePluginId.class), datasetConfig, null, splitsPointer, null);
  }

  private SplitsPointer getSplitsPointer(Map<String, List<Split>> splits) {
    int totalSplits = splits.values().stream()
        .map(List::size)
        .reduce(0, Integer::sum);
    return MaterializedSplitsPointer.of(0, getPartitionChunks(splits), totalSplits);
  }

  private List<PartitionChunkMetadata> getPartitionChunks(Map<String, List<Split>> splits) {
    return splits.entrySet().stream()
        .map(entry -> getPartitionChunk(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private PartitionChunkMetadata getPartitionChunk(String partitionValue, List<Split> splits) {

    ImmutableList.Builder<PartitionProtobuf.DatasetSplit> splitListBuilder = new ImmutableList.Builder<>();
    for (Split split : splits) {
      EasyProtobuf.EasyDatasetSplitXAttr splitExtended = split.toXAttr();

      PartitionProtobuf.DatasetSplit datasetSplit = PartitionProtobuf.DatasetSplit.newBuilder()
          .setSplitExtendedProperty(splitExtended.toByteString())
          .setSize(1)
          .setRecordCount(0)
          .build();
      splitListBuilder.add(datasetSplit);
    }

    PartitionProtobuf.PartitionValue partition = PartitionProtobuf.PartitionValue.newBuilder()
        .setColumn("dir0")
        .setStringValue(partitionValue)
        .build();

    PartitionProtobuf.PartitionChunk chunk = PartitionProtobuf.PartitionChunk.newBuilder()
        .setSize(splits.size())
        .setRowCount(0)
        .addPartitionValues(partition)
        .setSplitCount(splits.size())
        .build();

    List<PartitionProtobuf.DatasetSplit> datasetSplits = splitListBuilder.build();
    return new AbstractPartitionChunkMetadata(chunk) {
      @Override
      public Iterable<PartitionProtobuf.DatasetSplit> getDatasetSplits() {
        return datasetSplits;
      }

      @Override
      public boolean checkPartitionChunkMetadataConsistency() {
        return true;
      }
    };
  }

  private static class Split {
    private final String path;
    private final long lastModifiedTime;
    public Split(String path, long lastModifiedTime) {
      this.path = path;
      this.lastModifiedTime = lastModifiedTime;
    }

    public static Split of(String path, long lastModifiedTime) {
      return new Split(path, lastModifiedTime);
    }

    public String getPath() {
      return path;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public EasyProtobuf.EasyDatasetSplitXAttr toXAttr() {
      return EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
          .setPath(path)
          .setUpdateKey(FileProtobuf.FileSystemCachedEntity.newBuilder()
              .setPath(path)
              .setLastModificationTime(lastModifiedTime))
          .build();
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (!(other instanceof Split)) {
        return false;
      }

      Split otherSplit = (Split) other;
      return path.equals(otherSplit.path) && lastModifiedTime == otherSplit.lastModifiedTime;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(path, lastModifiedTime);
    }
  }
}
