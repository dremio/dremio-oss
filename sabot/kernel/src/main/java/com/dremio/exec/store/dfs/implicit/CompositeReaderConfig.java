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
package com.dremio.exec.store.dfs.implicit;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

public class CompositeReaderConfig {

  private final OperatorContext context;
  private final ImmutableList<SchemaPath> innerColumns;
  private final Map<String, Field> implicitColumns;

  private CompositeReaderConfig(
      OperatorContext context, List<SchemaPath> innerColumns, List<Field> implicitColumns) {
    super();
    this.context = context;
    this.innerColumns = ImmutableList.copyOf(innerColumns);
    this.implicitColumns =
        implicitColumns.stream().collect(Collectors.toMap(Field::getName, f -> f));
  }

  public List<SchemaPath> getInnerColumns() {
    return innerColumns;
  }

  public RecordReader wrapIfNecessary(
      BufferAllocator allocator, RecordReader innerReader, SplitAndPartitionInfo splitInfo) {
    return wrapIfNecessary(
        allocator, innerReader, splitInfo, new PartitionImplicitColumnValuesProvider());
  }

  public RecordReader wrapIfNecessary(
      BufferAllocator allocator,
      RecordReader innerReader,
      SplitAndPartitionInfo splitInfo,
      ImplicitColumnValuesProvider implicitFieldProvider) {
    if (implicitColumns.isEmpty()) {
      return innerReader;
    } else {
      final List<NameValuePair<?>> nameValuePairs =
          implicitFieldProvider.getImplicitColumnValues(
              allocator, splitInfo, implicitColumns, context.getOptions());
      return new AdditionalColumnsRecordReader(
          context, innerReader, nameValuePairs, allocator, splitInfo);
    }
  }

  public List<NameValuePair<?>> getPartitionNVPairs(
      final BufferAllocator allocator, final SplitAndPartitionInfo split) {
    PartitionImplicitColumnValuesProvider implicitFieldProvider =
        new PartitionImplicitColumnValuesProvider();
    return implicitFieldProvider.getImplicitColumnValues(
        allocator, split, implicitColumns, context.getOptions());
  }

  public static CompositeReaderConfig getCompound(
      OperatorContext context,
      BatchSchema schema,
      List<SchemaPath> selectedColumns,
      List<String> partitionColumnsList) {
    Set<String> allImplicitColumns = new HashSet<>();
    if (partitionColumnsList != null) {
      allImplicitColumns.addAll(partitionColumnsList);
    }
    allImplicitColumns.addAll(
        ImplicitFilesystemColumnFinder.getEnabledNonPartitionColumns(context.getOptions()));

    List<SchemaPath> remainingColumns = new ArrayList<>();
    Set<String> selectedImplicitColumns = new HashSet<>();
    for (SchemaPath p : selectedColumns) {
      if (p.getRootSegment().isLastPath()
          && allImplicitColumns.contains(p.getRootSegment().getPath())) {
        selectedImplicitColumns.add(p.getRootSegment().getPath());
      } else {
        remainingColumns.add(p);
      }
    }

    List<Field> implicitColumns =
        schema.getFields().stream()
            .filter(f -> selectedImplicitColumns.contains(f.getName()))
            .collect(Collectors.toList());

    return new CompositeReaderConfig(context, remainingColumns, implicitColumns);
  }
}
