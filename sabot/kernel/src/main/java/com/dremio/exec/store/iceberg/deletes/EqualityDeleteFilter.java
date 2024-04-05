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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;

/**
 * Exposes a filtering interface on top of a set of {@link EqualityDeleteHashTable} instances
 * suitable for use with RecordReader implementations - e.g. {@link
 * com.dremio.exec.store.parquet.UnifiedParquetReader}.
 */
@NotThreadSafe
public class EqualityDeleteFilter implements AutoCloseable {

  private final BufferAllocator allocator;
  private final LazyEqualityDeleteTableSupplier tablesSupplier;
  private final OperatorStats operatorStats;

  private OutputMutator mutator;
  private ArrowBuf validityBuf;
  private List<TableGroup> tableGroups;
  private int refCount;

  public EqualityDeleteFilter(
      BufferAllocator allocator,
      LazyEqualityDeleteTableSupplier tablesSupplier,
      int initialRefCount,
      OperatorStats operatorStats) {
    this.allocator = Preconditions.checkNotNull(allocator);
    this.tablesSupplier = Preconditions.checkNotNull(tablesSupplier);
    this.refCount = initialRefCount;
    this.operatorStats = operatorStats;
  }

  public void retain() {
    retain(1);
  }

  public void retain(int count) {
    refCount += count;
  }

  public void release(int count) {
    Preconditions.checkState(refCount >= count);
    refCount -= count;
    if (refCount == 0) {
      AutoCloseables.close(RuntimeException.class, this);
    }
  }

  public void release() {
    release(1);
  }

  public int refCount() {
    return refCount;
  }

  public void setup(OutputMutator mutator, ArrowBuf validityBuf) {
    this.mutator = Preconditions.checkNotNull(mutator);
    this.validityBuf = Preconditions.checkNotNull(validityBuf);

    // validate that all equality fields are present in the provided output mutator
    List<SchemaPath> missingFields =
        getAllEqualityFields().stream()
            .filter(p -> mutator.getVector(p.getAsUnescapedPath()) == null)
            .collect(Collectors.toList());
    Preconditions.checkState(
        missingFields.isEmpty(),
        String.format(
            "Missing equality delete filter fields in OutputMutator: %s",
            missingFields.stream().map(SchemaPath::toString).collect(Collectors.joining(", "))));

    // group tables by their equality field list
    Map<List<SchemaPath>, List<EqualityDeleteHashTable>> tablesByEqualityFields =
        tablesSupplier.get().stream()
            .collect(
                Collectors.groupingBy(
                    EqualityDeleteHashTable::getEqualityFields,
                    Collectors.mapping(v -> v, Collectors.toList())));

    // create a PivotDef for each table group and create the final list of TableGroups
    this.tableGroups =
        tablesByEqualityFields.entrySet().stream()
            .map(e -> new TableGroup(e.getValue(), createPivotDefForFields(e.getKey())))
            .collect(Collectors.toList());
  }

  public List<SchemaPath> getAllEqualityFields() {
    return tablesSupplier.getAllEqualityFields();
  }

  public void filter(int records) {
    int deleteCount = 0;
    for (TableGroup tableGroup : tableGroups) {
      deleteCount += filterOnTableGroup(records, tableGroup);
    }

    if (operatorStats != null) {
      operatorStats.addLongStat(TableFunctionOperator.Metric.NUM_EQ_DELETED_ROWS, deleteCount);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(tablesSupplier);
  }

  private PivotDef createPivotDefForFields(List<SchemaPath> equalityFields) {
    // TODO: does this work with nested fields?
    Preconditions.checkState(mutator != null, "setup must be called prior to using the filter");
    List<FieldVectorPair> fieldVectorPairs =
        equalityFields.stream()
            .map(f -> (FieldVector) mutator.getVector(f.getAsUnescapedPath()))
            .map(v -> new FieldVectorPair(v, v))
            .collect(Collectors.toList());

    return PivotBuilder.getBlockDefinition(fieldVectorPairs);
  }

  private int filterOnTableGroup(int records, TableGroup tableGroup) {
    int deleteCount = 0;

    try (FixedBlockVector fbv =
            new FixedBlockVector(allocator, tableGroup.pivotDef.getBlockWidth());
        VariableBlockVector vbv =
            new VariableBlockVector(allocator, tableGroup.pivotDef.getVariableCount());
        ArrowBuf outOrdinals = allocator.buffer((long) records * ORDINAL_SIZE)) {

      // compute the row-wise key pivot once for the TableGroup as key structure is the same for all
      // tables
      // in the group
      Pivots.pivot(tableGroup.pivotDef, records, fbv, vbv);

      // then probe each table
      for (EqualityDeleteHashTable table : tableGroup.tables) {
        table.find(records, fbv, vbv, outOrdinals);

        // for each row, if the key was found in the table, mark the row as invalid
        for (int i = 0; i < records; i++) {
          if (outOrdinals.getInt(i * 4L) != -1 && BitVectorHelper.get(validityBuf, i) == 1) {
            BitVectorHelper.unsetBit(validityBuf, i);
            deleteCount++;
          }
        }
      }
    }

    return deleteCount;
  }

  private static class TableGroup {

    public final List<EqualityDeleteHashTable> tables;
    public final PivotDef pivotDef;

    public TableGroup(List<EqualityDeleteHashTable> tables, PivotDef pivotDef) {
      this.tables = tables;
      this.pivotDef = pivotDef;
    }
  }
}
