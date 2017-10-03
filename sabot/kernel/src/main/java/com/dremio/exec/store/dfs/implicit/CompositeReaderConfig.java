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
package com.dremio.exec.store.dfs.implicit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader.Populator;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.BigIntNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.BitNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.DateMilliNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.Float4NameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.Float8NameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.IntNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.TimeMilliNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.TimeStampMilliNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.VarBinaryNameValuePair;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators.VarCharNameValuePair;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CompositeReaderConfig {

  private final ImmutableList<SchemaPath> innerColumns;
  private final ImmutableMap<String, FieldValuePair> partitionFieldMap;

  private CompositeReaderConfig(List<SchemaPath> innerColumns, List<FieldValuePair> partitionFields) {
    super();
    this.innerColumns = ImmutableList.copyOf(innerColumns);
    this.partitionFieldMap = FluentIterable.from(partitionFields).uniqueIndex(new Function<FieldValuePair, String>(){
      @Override
      public String apply(FieldValuePair input) {
        return input.field.getName();
      }});
  }

  public List<SchemaPath> getInnerColumns(){
    return innerColumns;
  }

  public boolean hasInnerColumns(){
    return !innerColumns.isEmpty();
  }

  public boolean hasPartitionColumns(){
    return !partitionFieldMap.isEmpty();
  }

  public RecordReader wrapIfNecessary(BufferAllocator allocator, RecordReader innerReader, DatasetSplit split){
    if(partitionFieldMap.isEmpty()){
      return innerReader;
    } else {
      Populator[] populators = new Populator[partitionFieldMap.size()];
      List<PartitionValue> values = split.getPartitionValuesList();
      int i = 0;
      for(PartitionValue v : values) {
        FieldValuePair p = partitionFieldMap.get(v.getColumn());
        if(p != null) {
          populators[i] = p.toPopulator(allocator, v);
          i++;
        }
      }
      return new AdditionalColumnsRecordReader(innerReader, populators);
    }
  }

  private static class FieldValuePair {
    private final Field field;

    public FieldValuePair(Field field) {
      super();
      this.field = field;
    }

    public Populator toPopulator(BufferAllocator allocator, PartitionValue value){
      return getPopulator(allocator, field, value);
    }
  }

  public static CompositeReaderConfig getCompound(BatchSchema schema, List<SchemaPath> selectedColumns, List<String> partColumnsList){

    if(partColumnsList == null || partColumnsList.isEmpty()){
      return new CompositeReaderConfig(selectedColumns, Collections.<FieldValuePair>emptyList());
    }

    Set<String> partitionColumns = new HashSet<>();
    for(String partitionColumn : partColumnsList){
      partitionColumns.add(partitionColumn);
    }

    List<SchemaPath> remainingColumns = new ArrayList<>();
    Set<String> selectedPartitionColumns = new HashSet<>();
    for(SchemaPath p : selectedColumns){
      if(p.getRootSegment().isLastPath() && partitionColumns.contains(p.getRootSegment().getPath())){
        selectedPartitionColumns.add(p.getRootSegment().getPath());
      } else {
        remainingColumns.add(p);
      }
    }

    final Map<String, Integer> partitionNamesToValues = new HashMap<>();
    for(int i =0; i < partColumnsList.size(); i++){
      String column = partColumnsList.get(i);
      if(selectedPartitionColumns.contains(column)){
        partitionNamesToValues.put(partColumnsList.get(i), i);
      }
    }

    List<FieldValuePair> pairs = new ArrayList<>();

    for(Field f : schema){
      Integer i = partitionNamesToValues.get(f.getName());
      if(i != null){
        pairs.add(new FieldValuePair(f));
      }
    }

    return new CompositeReaderConfig(remainingColumns, pairs);
  }


  public static Populator getPopulator(BufferAllocator allocator, Field field, PartitionValue partitionValue){
    final CompleteType type = CompleteType.fromField(field);
    switch(type.toMinorType()){
    case BIGINT:
      return new BigIntNameValuePair(field.getName(), partitionValue.getLongValue()).createPopulator();
    case BIT:
      return new BitNameValuePair(field.getName(), partitionValue.getBitValue()).createPopulator();
    case DATE:
      return new DateMilliNameValuePair(field.getName(), partitionValue.getLongValue()).createPopulator();
    case FLOAT4:
      return new Float4NameValuePair(field.getName(), partitionValue.getFloatValue()).createPopulator();
    case FLOAT8:
      return new Float8NameValuePair(field.getName(), partitionValue.getDoubleValue()).createPopulator();
    case INT:
      return new IntNameValuePair(field.getName(), partitionValue.getIntValue()).createPopulator();
    case TIME:
      return new TimeMilliNameValuePair(field.getName(), partitionValue.getIntValue()).createPopulator();
    case TIMESTAMP:
      return new TimeStampMilliNameValuePair(field.getName(), partitionValue.getLongValue()).createPopulator();
    case DECIMAL:
      return new TwosComplementValuePair(allocator, field, partitionValue.getBinaryValue().toByteArray()).createPopulator();
    case VARBINARY:
      return new VarBinaryNameValuePair(field.getName(), partitionValue.getBinaryValue().toByteArray()).createPopulator();
    case VARCHAR:
      return new VarCharNameValuePair(field.getName(), partitionValue.getStringValue()).createPopulator();
    default:
      throw new UnsupportedOperationException("Unable to return partition field: "  + Describer.describe(field));

    }
  }


}
