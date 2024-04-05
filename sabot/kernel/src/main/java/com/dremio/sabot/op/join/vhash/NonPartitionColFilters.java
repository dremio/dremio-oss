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

package com.dremio.sabot.op.join.vhash;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.HashTableFilterUtil;
import com.dremio.sabot.op.common.ht2.HashTableKeyReader;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.common.ht2.VectorPivotDef;
import com.dremio.sabot.op.join.vhash.spill.partition.DiskPartitionFilterHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class NonPartitionColFilters implements AutoCloseable {
  private final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NonPartitionColFilters.class);
  static final int MAX_VAL_LIST_FILTER_KEY_SIZE = 17;

  private final List<NonPartitionColFilter> nonPartitionColFilters;

  private final BufferAllocator allocator;
  private final List<RuntimeFilterProbeTarget> probeTargets;
  private final PivotDef pivotDef;
  private final int maxElements;

  public NonPartitionColFilters(
      BufferAllocator allocator,
      List<RuntimeFilterProbeTarget> probeTargets,
      PivotDef pivotDef,
      int maxElements) {
    this.allocator =
        allocator.newChildAllocator("nonpartition-col-filters", 0, allocator.getLimit());
    this.probeTargets = probeTargets;
    this.pivotDef = pivotDef;
    this.maxElements = maxElements;
    this.nonPartitionColFilters = build();
  }

  private static ArrowType getFieldType(
      final List<VectorPivotDef> vectorPivotDefs, final String fieldName) {
    return vectorPivotDefs.stream()
        .map(p -> p.getIncomingVector().getField())
        .filter(f -> f.getName().equalsIgnoreCase(fieldName))
        .map(f -> f.getType())
        .findAny()
        .orElse(null);
  }

  private void setFieldType(final ValueListFilterBuilder filterBuilder, final String fieldName) {
    // Check fixed and variable width vectorPivotDefs one by one
    ArrowType fieldType = getFieldType(pivotDef.getFixedPivots(), fieldName);
    byte precision = 0;
    byte scale = 0;
    if (fieldType == null) {
      fieldType = getFieldType(pivotDef.getVariablePivots(), fieldName);
      filterBuilder.setFixedWidth(false);
    }
    Preconditions.checkNotNull(fieldType, "Not able to find %s in build pivot", fieldName);
    if (fieldType instanceof ArrowType.Decimal) {
      precision = (byte) ((ArrowType.Decimal) fieldType).getPrecision();
      scale = (byte) ((ArrowType.Decimal) fieldType).getScale();
    }

    filterBuilder.setFieldType(Types.getMinorTypeForArrowType(fieldType), precision, scale);
  }

  private List<NonPartitionColFilter> build() {
    List<NonPartitionColFilter> nonPartitionColFilters = new ArrayList<>();

    for (int i = 0; i < probeTargets.size(); i++) {
      RuntimeFilterProbeTarget probeTarget = probeTargets.get(i);

      if (CollectionUtils.isEmpty(probeTarget.getNonPartitionBuildTableKeys())) {
        NonPartitionColFilter nonPartitionColFilter =
            new NonPartitionColFilter(
                probeTarget, Collections.emptyList(), Collections.emptyList());
        nonPartitionColFilters.add(nonPartitionColFilter);
        logger.warn("Ignoring empty non partition col filter(" + i + ")");
        continue;
      }

      final List<HashTableKeyReader> keyReaderList = new ArrayList<>();
      final List<ValueListFilterBuilder> valueListFilterBuilderList = new ArrayList<>();

      for (int colId = 0; colId < probeTarget.getNonPartitionBuildTableKeys().size(); colId++) {
        String fieldName = probeTarget.getNonPartitionBuildTableKeys().get(colId);

        if (StringUtils.isEmpty(fieldName)) {
          logger.warn("Ignoring empty col filedName(" + i + ", " + colId + ")");
          continue;
        }

        final boolean isBooleanField = pivotDef.isBoolField(fieldName);

        try (AutoCloseables.RollbackCloseable closeOnError =
            new AutoCloseables.RollbackCloseable()) {
          final HashTableKeyReader keyReader =
              new HashTableKeyReader.Builder()
                  .setBufferAllocator(allocator)
                  .setFieldsToRead(ImmutableList.of(fieldName))
                  .setPivot(pivotDef)
                  .setSetVarFieldLenInFirstByte(
                      true) // Set length at first byte, to avoid comparison issues for different
                  // size values.
                  .setMaxKeySize(
                      MAX_VAL_LIST_FILTER_KEY_SIZE) // Max key size 16, excluding one byte for
                  // validity bits.
                  .build();
          closeOnError.add(keyReader);

          ValueListFilterBuilder filterBuilder =
              new ValueListFilterBuilder(
                  allocator,
                  maxElements,
                  isBooleanField ? 0 : keyReader.getEffectiveKeySize(),
                  isBooleanField);
          filterBuilder.setup();
          filterBuilder.setFieldName(probeTarget.getNonPartitionProbeTableKeys().get(colId));
          filterBuilder.setName(Thread.currentThread().getName());
          closeOnError.add(filterBuilder);
          setFieldType(filterBuilder, fieldName);

          closeOnError.commit();

          keyReaderList.add(keyReader);
          valueListFilterBuilderList.add(filterBuilder);
        } catch (Exception e) {
          logger.warn(
              "Unable to prepare value list filter for {} because {}", fieldName, e.getMessage());
        }
      }
      Preconditions.checkState(keyReaderList.size() == valueListFilterBuilderList.size());
      NonPartitionColFilter nonPartitionColFilter =
          new NonPartitionColFilter(probeTarget, keyReaderList, valueListFilterBuilderList);
      nonPartitionColFilters.add(nonPartitionColFilter);
    }

    return nonPartitionColFilters;
  }

  public void finalizeValueListFilters() {
    for (int i = 0; i < probeTargets.size(); i++) {
      NonPartitionColFilter nonPartitionColFilter = nonPartitionColFilters.get(i);
      List<ValueListFilterBuilder> valueListFilterBuilderList =
          nonPartitionColFilter.getValueListFilterBuilderList();
      List<HashTableKeyReader> keyReaderList = nonPartitionColFilter.getKeyReaderList();
      Preconditions.checkState(keyReaderList.size() == valueListFilterBuilderList.size());

      List<ValueListFilter> valueListFilterList = new ArrayList<>();
      for (int col = 0; col < keyReaderList.size(); col++) {
        ValueListFilterBuilder valueListFilterBuilder = valueListFilterBuilderList.get(col);
        ValueListFilter valueListFilter = valueListFilterBuilder.build();
        valueListFilterList.add(valueListFilter);
      }
      nonPartitionColFilter.setValueListFilters(valueListFilterList);
    }
  }

  public void prepareValueListFilters(HashTable hashTable) {
    for (int i = 0; i < nonPartitionColFilters.size(); i++) {
      NonPartitionColFilter nonPartitionColFilter = nonPartitionColFilters.get(i);
      HashTableFilterUtil.prepareValueListFilters(
          nonPartitionColFilter.getKeyReaderList(),
          nonPartitionColFilter.getValueListFilterBuilderList(),
          pivotDef,
          hashTable);
    }
  }

  // For disk partition
  public void prepareValueListFilters(
      FixedBlockVector pivotedFixedBlockVector,
      VariableBlockVector pivotedVariableBlockVector,
      int pivotShift,
      int records,
      ArrowBuf sv2) {
    for (int i = 0; i < nonPartitionColFilters.size(); i++) {
      NonPartitionColFilter nonPartitionColFilter = nonPartitionColFilters.get(i);
      DiskPartitionFilterHelper.prepareValueListFilters(
          nonPartitionColFilter.getKeyReaderList(),
          nonPartitionColFilter.getValueListFilterBuilderList(),
          pivotDef,
          pivotedFixedBlockVector,
          pivotedVariableBlockVector,
          pivotShift,
          records,
          sv2);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(nonPartitionColFilters);
    AutoCloseables.close(allocator);
  }

  public List<ValueListFilter> getValueListFilters(
      int index, RuntimeFilterProbeTarget probeTarget) {
    NonPartitionColFilter nonPartitionColFilter = nonPartitionColFilters.get(index);
    Preconditions.checkState(nonPartitionColFilter.getProbeTarget() == probeTarget);
    return nonPartitionColFilter.getValueListFilters();
  }

  public class NonPartitionColFilter implements AutoCloseable {
    private final RuntimeFilterProbeTarget probeTarget;
    private final List<HashTableKeyReader> keyReaderList;
    private final List<ValueListFilterBuilder> valueListFilterBuilderList;
    private List<ValueListFilter> valueListFilters = null;

    public NonPartitionColFilter(
        RuntimeFilterProbeTarget probeTarget,
        List<HashTableKeyReader> keyReaderList,
        List<ValueListFilterBuilder> valueListFilterBuilderList) {
      this.probeTarget = probeTarget;
      this.keyReaderList = keyReaderList;
      this.valueListFilterBuilderList = valueListFilterBuilderList;
    }

    public void setValueListFilters(List<ValueListFilter> valueListFilters) {
      this.valueListFilters = valueListFilters;
    }

    public List<ValueListFilter> getValueListFilters() {
      return valueListFilters;
    }

    public List<HashTableKeyReader> getKeyReaderList() {
      return keyReaderList;
    }

    public RuntimeFilterProbeTarget getProbeTarget() {
      return probeTarget;
    }

    public List<ValueListFilterBuilder> getValueListFilterBuilderList() {
      return valueListFilterBuilderList;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(keyReaderList, valueListFilterBuilderList);
      AutoCloseables.close(valueListFilters);
    }
  }
}
