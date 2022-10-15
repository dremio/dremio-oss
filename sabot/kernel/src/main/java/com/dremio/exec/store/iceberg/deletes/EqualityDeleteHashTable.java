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

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;

/**
 * A simple wrapper around an Arrow-based hash table used to store equality delete keys.  Both insertions and lookups
 * are done at a record-batch granularity.
 */
@NotThreadSafe
public class EqualityDeleteHashTable implements AutoCloseable {

  private final BufferAllocator allocator;
  private final List<SchemaPath> equalityFields;

  private LBlockHashTable table;

  public EqualityDeleteHashTable(BufferAllocator allocator, LBlockHashTable table, List<SchemaPath> equalityFields) {
    this.allocator = allocator;
    this.table = Preconditions.checkNotNull(table);
    this.equalityFields = Preconditions.checkNotNull(equalityFields);
  }

  public List<SchemaPath> getEqualityFields() {
    return equalityFields;
  }

  public int size() {
    return table.size();
  }

  public void find(int records, FixedBlockVector fbv, VariableBlockVector vbv, ArrowBuf outOrdinals) {
    try (ArrowBuf hashValues = allocator.buffer(records * 8L)) {
      table.computeHash(records, fbv.getBuf(), vbv.getBuf(), 0, hashValues);
      table.find(records, fbv.getBuf(), vbv.getBuf(), hashValues, outOrdinals);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(table);
    table = null;
  }

  public static class Builder implements AutoCloseable {

    private static final int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

    private final BufferAllocator allocator;
    private final List<SchemaPath> equalityFields;
    private final PivotDef buildPivot;
    private final Stopwatch insertTimer = Stopwatch.createUnstarted();

    private LBlockHashTable table;

    public Builder(BufferAllocator allocator, List<SchemaPath> equalityFields, List<FieldVector> equalityVectors,
        int tableSize, int batchSize) {
      Preconditions.checkArgument(equalityFields.size() > 0, "equalityFields is empty");
      Preconditions.checkArgument(equalityFields.size() == equalityVectors.size(),
          "equalityFields and equalityVectors list sizes do not match");
      this.allocator = Preconditions.checkNotNull(allocator);
      this.equalityFields = Preconditions.checkNotNull(equalityFields);

      List<FieldVectorPair> fieldVectorPairs = equalityVectors.stream()
          .map(f -> new FieldVectorPair(f, f))
          .collect(Collectors.toList());
      this.buildPivot = PivotBuilder.getBlockDefinition(fieldVectorPairs);

      this.table = new LBlockHashTable(new HashTable.HashTableCreateArgs(
          HashConfig.getDefault(),
          buildPivot,
          allocator,
          tableSize,
          INITIAL_VAR_FIELD_AVERAGE_SIZE,
          false,
          batchSize,
          null,  // we want IS DISTINCT FROM comparison semantics
          false));
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(table);
    }

    public void insertBatch(int records, ArrowBuf outOrdinals) {
      try (FixedBlockVector fbv = new FixedBlockVector(allocator, buildPivot.getBlockWidth());
          VariableBlockVector var = new VariableBlockVector(allocator, buildPivot.getVariableCount())) {
        insertTimer.start();

        // STEP 1: first we pivot keys from columnar to a row-wise form
        Pivots.pivot(buildPivot, records, fbv, var);

        try (ArrowBuf hashValues = allocator.buffer(records * 8L)) {
          // STEP 2: then we do the hash computation on entire batch
          table.computeHash(records, fbv.getBuf(), var.getBuf(), 0, hashValues);

          // STEP 3: then we insert keys into hash table
          int recordsAdded = table.add(records, fbv.getBuf(), var.getBuf(), hashValues, outOrdinals);

          if (recordsAdded < records) {
            throw new OutOfMemoryException(String.format("Only %d records out of %d were added to the HashTable",
                recordsAdded, records));
          }
        }

        insertTimer.stop();
      }
    }

    public EqualityDeleteHashTable build() {
      EqualityDeleteHashTable resultTable = new EqualityDeleteHashTable(allocator, table, equalityFields);
      // null out table ref - resultTable now owns it
      table = null;
      return resultTable;
    }
  }
}
