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
package com.dremio.sabot;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Copy of CustomHashAggDataGenerator but does Decimal computations. */
public class CustomHashAggDataGeneratorDecimal implements Generator {
  private static final FieldType DECIMAL_FIELD_TYPE =
      FieldType.nullable(new ArrowType.Decimal(38, 9, 128));
  private static final ArrowType.Decimal DECIMAL_ARROWTYPE =
      (ArrowType.Decimal) DECIMAL_FIELD_TYPE.getType();
  private static final CompleteType DECIMAL_COMPLETE_TYPE =
      new CompleteType(DECIMAL_ARROWTYPE, new ArrayList<>());

  private static final Field DECIMAL_KEY = DECIMAL_COMPLETE_TYPE.toField("DECIMAL_KEY");

  private static final Field DECIMAL_MEASURE = DECIMAL_COMPLETE_TYPE.toField("DECIMAL_MEASURE");

  /* arrays on heap that will store column values as we generate data for the schema */
  private BigDecimal[] decimalKeyValues;

  private BigDecimal[] decimalMeasureValues;

  /* vectors in the input container to feed the data into operator */
  private DecimalVector decimalKey;

  private DecimalVector decimalMeasure;

  private VectorContainer container;

  private int position;
  private int batches;
  private final boolean largeVarChars;

  private static final int GROUP_REPEAT_PER_BATCH = 5;
  /* simplify data generation by using a fixed internal batch size */
  private static final int BATCH_SIZE = 1000;
  /* number of unique keys per batch == number of groups per batch */
  private static final int GROUPS_PER_BATCH = BATCH_SIZE / GROUP_REPEAT_PER_BATCH;
  private static final BigDecimal DECIMAL_BASEVALUE = new BigDecimal("10.254567123");
  private static final BigDecimal DECIMAL_INCREMENT = new BigDecimal("1.0");
  private static final int GROUP_INTERVAL_PER_BATCH = 20;

  private final int numRows;
  private final Map<Key, Value> aggregatedResults = new HashMap<>();

  public CustomHashAggDataGeneratorDecimal(
      int numRows, BufferAllocator allocator, final boolean largeVarChars) {
    Preconditions.checkState(
        numRows > 0 && numRows % BATCH_SIZE == 0,
        "ERROR: total number of rows should be greater than 0");
    this.numRows = numRows;
    this.batches = numRows / BATCH_SIZE;
    this.largeVarChars = largeVarChars;
    createBigSchemaAndInputContainer(allocator);
    buildInputTableDataAndResultset();
  }

  private void createBigSchemaAndInputContainer(final BufferAllocator allocator) {
    final BatchSchema schema =
        BatchSchema.newBuilder().addField(DECIMAL_KEY).addField(DECIMAL_MEASURE).build();

    container = VectorContainer.create(allocator, schema);

    decimalKey = container.addOrGet(DECIMAL_KEY);

    decimalMeasure = container.addOrGet(DECIMAL_MEASURE);
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public int next(int records) {
    if (position == decimalKeyValues.length) {
      return 0;
    }
    records = Math.min(records, decimalKeyValues.length - position);
    container.allocateNew();
    for (int i = 0; i < records; i++) {
      final int absoluteRecordIndex = position + i;
      /* populate vectors in incoming batch vectors for key column data */
      decimalKey.setSafe(i, decimalKeyValues[absoluteRecordIndex]);

      /* populate vectors in incoming batch for accumulator column data */
      decimalMeasure.setSafe(i, decimalMeasureValues[absoluteRecordIndex]);
    }

    container.setAllCount(records);
    position += records;
    return records;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container);
  }

  /**
   * Currently we generate a simple patter where we fix the number of times a key will repeat and
   * after how many records it will repeat.
   */
  private void buildInputTableDataAndResultset() {
    decimalKeyValues = new BigDecimal[numRows];

    decimalMeasureValues = new BigDecimal[numRows];

    int batch = 0;
    /* build batch by batch */
    while (batch != batches) {
      int startPoint = batch * BATCH_SIZE;
      int pos = startPoint;
      for (int i = startPoint; i < (startPoint + BATCH_SIZE); i++) {
        if (i == 0) {
          /* key columns */
          decimalKeyValues[0] = DECIMAL_BASEVALUE;

          /* measure columns */
          decimalMeasureValues[0] = DECIMAL_BASEVALUE;
        } else {
          if (i == pos + (GROUP_INTERVAL_PER_BATCH * GROUP_REPEAT_PER_BATCH)) {
            pos = i;
          }

          /* key columns */
          if (i < pos + GROUP_INTERVAL_PER_BATCH) {
            /* generate unique key column values */
            decimalKeyValues[i] = decimalKeyValues[i - 1].add(DECIMAL_INCREMENT);
          } else {
            /* generate duplicate key column values */
            decimalKeyValues[i] = decimalKeyValues[i - GROUP_INTERVAL_PER_BATCH];
          }

          /* measure columns */
          decimalMeasureValues[i] = decimalMeasureValues[i - 1].add(DECIMAL_INCREMENT);
        }

        /* compute the hashagg results as we build the data */
        final Key k = new Key(decimalKeyValues[i]);
        Value v = aggregatedResults.get(k);
        if (v == null) {
          v =
              new Value(
                  decimalMeasureValues[i], decimalMeasureValues[i], decimalMeasureValues[i], 1);
          aggregatedResults.put(k, v);
        } else {

          v.sumDecimal = v.sumDecimal.add(decimalMeasureValues[i]);
          v.minDecimal =
              decimalMeasureValues[i].compareTo(v.minDecimal) < 0
                  ? decimalMeasureValues[i]
                  : v.minDecimal;
          v.maxDecimal =
              decimalMeasureValues[i].compareTo(v.maxDecimal) > 0
                  ? decimalMeasureValues[i]
                  : v.maxDecimal;
          v.countDecimal++;
        }
      }

      batch++;
    }

    Preconditions.checkArgument(
        aggregatedResults.size() == (GROUPS_PER_BATCH * batches), "result table built incorrectly");
  }

  public Fixtures.Table getExpectedGroupsAndAggregations() {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[GROUPS_PER_BATCH * batches];
    Iterator iterator = aggregatedResults.entrySet().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final Map.Entry<Key, Value> pair = (Map.Entry<Key, Value>) iterator.next();
      final Key k = pair.getKey();
      final Value v = pair.getValue();
      rows[row] = tr(k.decimalKey, v.sumDecimal, v.minDecimal, v.maxDecimal, v.sumDecimal);
      row++;
    }
    return t(th("DECIMAL_KEY", "SUM_DECIMAL", "MIN_DECIMAL", "MAX_DECIMAL", "SUM0_DECIMAL"), rows)
        .orderInsensitive();
  }

  private static final class Key {
    private final BigDecimal decimalKey;

    Key(final BigDecimal decimalKey) {
      this.decimalKey = decimalKey;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Key)) {
        return false;
      }

      final Key k = (Key) other;
      return (this.decimalKey.compareTo(((Key) other).decimalKey) == 0);
    }

    @Override
    public int hashCode() {
      return decimalKey.hashCode();
    }
  }

  private static class Value {
    private BigDecimal sumDecimal;
    private BigDecimal minDecimal;
    private BigDecimal maxDecimal;
    private long countDecimal;

    Value(
        final BigDecimal sumDecimal,
        final BigDecimal minDecimal,
        final BigDecimal maxDecimal,
        final long countDecimal) {
      this.sumDecimal = sumDecimal;
      this.minDecimal = minDecimal;
      this.maxDecimal = maxDecimal;
      this.countDecimal = countDecimal;
    }
  }
}
