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
package com.dremio.exec.store.parquet;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SimpleIntVector;

/** Generate monotonically increasing integers */
public class BigIntAutoIncrementer
    implements AdditionalColumnsRecordReader.Populator, AutoCloseable {

  private static long INCREMENT = 1;
  private BigIntVector vector;
  private final String columnName;
  private final int batchSize;
  private long currentRowIndex = 0;
  private long rowIndexBase = 0;
  private final SimpleIntVector deltas;

  public BigIntAutoIncrementer(String columnName, int batchSize, SimpleIntVector deltas) {
    Preconditions.checkArgument(
        columnName != null && !columnName.isEmpty(), "Column name is required");
    this.columnName = columnName;
    this.batchSize = batchSize;
    this.deltas = deltas;
  }

  public void setRowIndexBase(long rowIndexBase) {
    this.rowIndexBase = rowIndexBase;
  }

  @Override
  public void setup(OutputMutator output) {
    vector = (BigIntVector) output.getVector(columnName);
    if (vector == null) {
      vector = output.addField(CompleteType.BIGINT.toField(columnName), BigIntVector.class);
    }
  }

  @Override
  public void populate(final int count) {
    // For every populate call, re-allocate memory.
    vector.allocateNew(batchSize);

    if (count == 0) {
      vector.setValueCount(count);
      return;
    }

    if (deltas != null && deltas.getValueCount() > 0) {
      populateWithDeltas(count);
      return;
    }

    for (int i = 0; i < count; i++) {
      vector.setSafe(i, currentRowIndex + rowIndexBase);
      currentRowIndex += INCREMENT;
    }

    vector.setValueCount(count);
  }

  private void populateWithDeltas(final int count) {
    for (int i = 0; i < count; i++) {
      // Add filtered row count
      currentRowIndex += deltas.get(i);
      vector.setSafe(i, currentRowIndex + rowIndexBase);
      currentRowIndex += INCREMENT;
    }
    vector.setValueCount(count);
  }

  @Override
  public void allocate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {}
}
