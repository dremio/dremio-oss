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

package com.dremio.exec.store.dfs.easy;

import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.ImplicitColumnValuesProvider;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.TestPartitionImplicitColumnValuesProvider;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableMap;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

public class TestEasyImplicitColumnValuesProvider
    extends TestPartitionImplicitColumnValuesProvider {

  private static final String SPLIT_FILE = "split";
  private static final String SPLIT_DIRS = "100/200/300/";
  private static final String SPLIT_PATH = SPLIT_DIRS + SPLIT_FILE;
  private static final long SPLIT_MOD_TIME =
      LocalDateTime.of(2023, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1000;

  @Test
  public void testModifiedTime() {
    String modTimeColumn =
        ImplicitFilesystemColumnFinder.getModifiedTimeColumnName(OPTION_RESOLVER);
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            modTimeColumn, Field.nullable(modTimeColumn, Types.MinorType.BIGINT.getType()));
    Map<String, Object> expectedImplicitValues = ImmutableMap.of(modTimeColumn, SPLIT_MOD_TIME);

    validate(implicitColumns, ImmutableMap.of(), expectedImplicitValues);
  }

  @Test
  public void testFilePath() {
    String fileColumn = ImplicitFilesystemColumnFinder.getFileColumnName(OPTION_RESOLVER);
    Map<String, Field> implicitColumns =
        ImmutableMap.of(fileColumn, Field.nullable(fileColumn, Types.MinorType.VARCHAR.getType()));
    Map<String, Object> expectedImplicitValues = ImmutableMap.of(fileColumn, SPLIT_FILE);

    validate(implicitColumns, ImmutableMap.of(), expectedImplicitValues);
  }

  @Test
  public void testUpdateColumn() {
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            IncrementalUpdateUtils.UPDATE_COLUMN,
            Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, Types.MinorType.BIGINT.getType()));
    Map<String, Object> expectedImplicitValues =
        ImmutableMap.of(IncrementalUpdateUtils.UPDATE_COLUMN, SPLIT_MOD_TIME);

    validate(implicitColumns, ImmutableMap.of(), expectedImplicitValues);
  }

  @Test
  public void testEasyImplicitsWithPartitions() {
    String modTimeColumn =
        ImplicitFilesystemColumnFinder.getModifiedTimeColumnName(OPTION_RESOLVER);
    String fileColumn = ImplicitFilesystemColumnFinder.getFileColumnName(OPTION_RESOLVER);
    Map<String, Field> implicitColumns =
        ImmutableMap.of(
            IMPLICIT_1,
            Field.nullable(IMPLICIT_1, Types.MinorType.VARCHAR.getType()),
            IMPLICIT_3,
            Field.nullable(IMPLICIT_3, Types.MinorType.INT.getType()),
            modTimeColumn,
            Field.nullable(modTimeColumn, Types.MinorType.BIGINT.getType()),
            fileColumn,
            Field.nullable(fileColumn, Types.MinorType.VARCHAR.getType()));
    Map<String, Object> partitionImplicitValues =
        ImmutableMap.of(
            IMPLICIT_1, "100",
            IMPLICIT_2, 200,
            IMPLICIT_3, 300);
    Map<String, Object> expectedImplicitValues =
        ImmutableMap.of(
            IMPLICIT_1,
            "100",
            IMPLICIT_3,
            300,
            modTimeColumn,
            SPLIT_MOD_TIME,
            fileColumn,
            SPLIT_FILE);

    validate(implicitColumns, partitionImplicitValues, expectedImplicitValues);
  }

  @Override
  protected ImplicitColumnValuesProvider getProvider() {
    return new EasyImplicitColumnValuesProvider(SPLIT_PATH);
  }

  @Override
  protected SplitAndPartitionInfo createSplitAndPartitionInfo(Map<String, Object> values) {
    List<PartitionProtobuf.PartitionValue> partitionValues =
        values.entrySet().stream()
            .map(entry -> createPartitionValue(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
            .addAllValues(partitionValues)
            .build();

    EasyProtobuf.EasyDatasetSplitXAttr splitExtended =
        EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(SPLIT_PATH)
            .setStart(0)
            .setLength(1)
            .setUpdateKey(
                FileProtobuf.FileSystemCachedEntity.newBuilder()
                    .setPath(SPLIT_PATH)
                    .setLastModificationTime(SPLIT_MOD_TIME))
            .build();
    PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo =
        PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
            .setExtendedProperty(splitExtended.toByteString())
            .build();

    return new SplitAndPartitionInfo(partitionInfo, splitInfo);
  }
}
