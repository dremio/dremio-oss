/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

/**
 * Used to generate data for testing hash agg spilling
 * functionality. As part of further development (non-contraction
 * etc), this should probably be extended to work with a schema
 * provided by the consumer.
 */
public class CustomHashAggDataGenerator implements Generator {

  /* fixed width dimensions */
  private final static Field FIXKEY1 = CompleteType.INT.toField("FIXKEY1");
  private final static Field FIXKEY2 = CompleteType.BIGINT.toField("FIXKEY2");

  /* variable width dimensions */
  private final static Field VARKEY1 = CompleteType.VARCHAR.toField("VARKEY1");

  /* Measures */
  private final static Field MEASURE1 = CompleteType.INT.toField("MEASURE1");
  private final static Field MEASURE2 = CompleteType.BIGINT.toField("MEASURE2");
  private final static Field MEASURE3 = CompleteType.FLOAT.toField("MEASURE3");
  private final static Field MEASURE4 = CompleteType.DOUBLE.toField("MEASURE4");

  /* input table */
  private Integer[] fixKeyValues1;
  private Long[] fixKeyValues2;
  private String[] varKeyValues1;
  private Integer[] measure1;
  private Long[] measure2;
  private Float[] measure3;
  private Double[] measure4;

  private final VectorContainer container;

  /* fixed width key columns  */
  private final IntVector fixkey1;
  private final BigIntVector fixkey2;

  /* variable width key columns */
  private final VarCharVector varkey1;

  /* measure columns */
  private final IntVector m1;
  private final BigIntVector m2;
  private final Float4Vector m3;
  private final Float8Vector m4;

  private int position;
  private int batches;
  private final boolean largeVarChars;

  private static final int GROUP_REPEAT_PER_BATCH = 5;
  /* simplify data generation by using a fixed internal batch size */
  private static final int BATCH_SIZE = 1000;
  /* number of unique keys per batch == number of groups per batch */
  private static final int GROUPS_PER_BATCH = BATCH_SIZE / GROUP_REPEAT_PER_BATCH;
  private static final int INT_BASEVALUE = 100;
  private static final int INT_INCREMENT = 10;
  private static final long BIGINT_BASEVALUE = 12345678990L;
  private static final long BIGINT_INCREMENT = 25L;
  private static final float FLOAT_BASEVALUE = 100.5F;
  private static final float FLOAT_INCREMENT = 2.5F;
  private static final double DOUBLE_BASEVALUE = 12345.25D;
  private static final double DOUBLE_INCREMENT = 100.25D;
  private static final String VARCHAR_BASEVALUE = "Dremio-3.0";
  private static final int GROUP_INTERVAL_PER_BATCH = 20;

  private final HashMap<Key, Value> aggregatedResults = new HashMap<>();

  public CustomHashAggDataGenerator(int numRows, BufferAllocator allocator,
                                    final boolean largeVarChars) {
    Preconditions.checkState(numRows > 0 && numRows%BATCH_SIZE == 0,
      "ERROR: total number of rows should be greater than 0");

    BatchSchema schema = BatchSchema.newBuilder()
      .addField(FIXKEY1)
      .addField(FIXKEY2)
      .addField(VARKEY1)
      .addField(MEASURE1)
      .addField(MEASURE2)
      .addField(MEASURE3)
      .addField(MEASURE4)
      .build();

    container = VectorContainer.create(allocator, schema);
    fixkey1 = container.addOrGet(FIXKEY1);
    fixkey2 = container.addOrGet(FIXKEY2);
    varkey1 = container.addOrGet(VARKEY1);
    m1 = container.addOrGet(MEASURE1);
    m2 = container.addOrGet(MEASURE2);
    m3 = container.addOrGet(MEASURE3);
    m4 = container.addOrGet(MEASURE4);

    this.batches = numRows/BATCH_SIZE;
    this.largeVarChars = largeVarChars;
    buildInputDataset(numRows);
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
    if (position == fixKeyValues1.length) {
      return 0;
    }
    records = Math.min(records, fixKeyValues1.length - position);
    container.allocateNew();

    for (int i = 0; i < records; i++) {
      final int absoluteRecordIndex = position + i;
      fixkey1.setSafe(i, fixKeyValues1[absoluteRecordIndex]);
      fixkey2.setSafe(i, fixKeyValues2[absoluteRecordIndex]);
      byte[] valueBytes = varKeyValues1[absoluteRecordIndex].getBytes();
      varkey1.setSafe(i, valueBytes, 0, valueBytes.length);
      m1.setSafe(i, measure1[absoluteRecordIndex]);
      m2.setSafe(i, measure2[absoluteRecordIndex]);
      m3.setSafe(i, measure3[absoluteRecordIndex]);
      m4.setSafe(i, measure4[absoluteRecordIndex]);
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
   * Currently we generate a simple patter where we fix the number of times
   * a key will repeat and after how many records it will repeat.
   *
   * @param numRows total number of rows in desired dataset
   */
  private void buildInputDataset(int numRows) {
    fixKeyValues1 = new Integer[numRows];
    fixKeyValues2 = new Long[numRows];
    varKeyValues1 = new String[numRows];
    measure1 = new Integer[numRows];
    measure2 = new Long[numRows];
    measure3 = new Float[numRows];
    measure4 = new Double[numRows];
    int batch = 0;
    /* build batch by batch */
    while (batch != batches) {
      int startPoint = batch * BATCH_SIZE;
      int pos = startPoint;
      for (int i = startPoint; i < (startPoint + BATCH_SIZE); i++) {
        if (i == 0) {
          /* key columns */
          fixKeyValues1[0] = INT_BASEVALUE;
          fixKeyValues2[0] = BIGINT_BASEVALUE;
          varKeyValues1[0] = VARCHAR_BASEVALUE;

          /* measure columns */
          measure1[0] = INT_BASEVALUE;
          measure2[0] = BIGINT_BASEVALUE;
          measure3[0] = FLOAT_BASEVALUE;
          measure4[0] = DOUBLE_BASEVALUE;
        } else {
          if (i == pos + (GROUP_INTERVAL_PER_BATCH * GROUP_REPEAT_PER_BATCH)) {
            pos = i;
          }

          /* key columns */
          if (i < pos + GROUP_INTERVAL_PER_BATCH) {
            /* generate unique column values */
            fixKeyValues1[i] = fixKeyValues1[i-1] + 1;
            fixKeyValues2[i] = fixKeyValues2[i-1] + 1L;
            if (largeVarChars) {
              /* length of varchars is directly proportional to the desired
               * number of rows we have to generate in the dataset.
               */
              varKeyValues1[i] = varKeyValues1[i-1] + String.format("%03d", 1);
            } else {
              varKeyValues1[i] = VARCHAR_BASEVALUE + RandomStringUtils.randomAlphabetic(10);
            }
          } else {
            /* generate duplicate column values */
            fixKeyValues1[i] = fixKeyValues1[i - GROUP_INTERVAL_PER_BATCH];
            fixKeyValues2[i] = fixKeyValues2[i - GROUP_INTERVAL_PER_BATCH];
            varKeyValues1[i] = varKeyValues1[i - GROUP_INTERVAL_PER_BATCH];
          }

          /* measure columns */
          measure1[i] = measure1[i-1] + INT_INCREMENT;
          measure2[i] = measure2[i-1] + BIGINT_INCREMENT;
          measure3[i] = measure3[i-1] + FLOAT_INCREMENT;
          measure4[i] = measure4[i-1] + DOUBLE_INCREMENT;
        }

        /* compute the hashagg results as we build the data */
        final Key k = new Key(fixKeyValues1[i], fixKeyValues2[i], varKeyValues1[i]);
        Value v = aggregatedResults.get(k);
        if (v == null) {
          v = new Value(measure1[i], measure1[i], measure1[i], 1,
                        measure2[i], measure2[i], measure2[i], 1,
                        measure3[i], measure3[i], measure3[i], 1,
                        measure4[i], measure4[i], measure4[i], 1);
          aggregatedResults.put(k, v);
        } else {
          v.sum_m1 += measure1[i];
          v.min_m1 = (measure1[i] < v.min_m1) ? measure1[i] : v.min_m1;
          v.max_m1 = (measure1[i] > v.max_m1) ? measure1[i] : v.max_m1;
          v.count_m1++;

          v.sum_m2 += measure2[i];
          v.min_m2 = (measure2[i] < v.min_m2) ? measure2[i] : v.min_m2;
          v.max_m2 = (measure2[i] > v.max_m2) ? measure2[i] : v.max_m2;
          v.count_m2++;

          v.sum_m3 += measure3[i];
          v.min_m3 = (measure3[i] < v.min_m3) ? measure3[i] : v.min_m3;
          v.max_m3 = (measure3[i] > v.max_m3) ? measure3[i] : v.max_m3;
          v.count_m3++;

          v.sum_m4 += measure4[i];
          v.min_m4 = (measure4[i] < v.min_m4) ? measure4[i] : v.min_m4;
          v.max_m4 = (measure4[i] > v.max_m4) ? measure4[i] : v.max_m4;
          v.count_m4++;
        }
      }

      batch++;
    }

    Preconditions.checkArgument(aggregatedResults.size() == (GROUPS_PER_BATCH * batches), "result table built incorrectly");
  }

  public Fixtures.Table getExpectedGroupsAndAggregations() {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[GROUPS_PER_BATCH * batches];
    Iterator iterator = aggregatedResults.entrySet().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final Map.Entry<Key, Value> pair = (Map.Entry<Key, Value>)iterator.next();
      final Key k = pair.getKey();
      final Value v = pair.getValue();
      rows[row] = tr(k.fix1, k.fix2, k.var1, v.sum_m1, v.min_m1, v.max_m1,
                     v.sum_m2, v.min_m2, v.max_m2, v.sum_m3, v.min_m3, v.max_m3,
                     v.sum_m4, v.min_m4, v.max_m4);
      row++;
    }
    return t(th("FIXKEY1", "FIXKEY2", "VARKEY1",
                "SUM_M1", "MIN_M1", "MAX_M1",
                "SUM_M2", "MIN_M2", "MAX_M2",
                "SUM_M3", "MIN_M3", "MAX_M3",
                "SUM_M4", "MIN_M4", "MAX_M4"),
             rows).orderInsensitive();
  }

  public Fixtures.Table getExpectedGroupsAndAggregationsWithCount() {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[GROUPS_PER_BATCH * batches];
    Iterator iterator = aggregatedResults.entrySet().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final Map.Entry<Key, Value> pair = (Map.Entry<Key, Value>)iterator.next();
      final Key k = pair.getKey();
      final Value v = pair.getValue();
      rows[row] = tr(k.fix1, k.fix2, k.var1, v.sum_m1, v.min_m1, v.max_m1, v.count_m1,
                     v.sum_m2, v.min_m2, v.max_m2, v.count_m2, v.sum_m3, v.min_m3, v.max_m3,
                     v.count_m3, v.sum_m4, v.min_m4, v.max_m4, v.count_m4);
      row++;
    }
    return t(th("FIXKEY1", "FIXKEY2", "VARKEY1",
                "SUM_M1", "MIN_M1", "MAX_M1", "COUNT_M1",
                "SUM_M2", "MIN_M2", "MAX_M2", "COUNT_M2",
                "SUM_M3", "MIN_M3", "MAX_M3", "COUNT_M3",
                "SUM_M4", "MIN_M4", "MAX_M4", "COUNT_M4"),
             rows).orderInsensitive();
  }

  private static class Key {
    private final int fix1;
    private final long fix2;
    private final String var1;

    Key(final int fix1,
        final long fix2,
        final String var1) {
      this.fix1 = fix1;
      this.fix2 = fix2;
      this.var1 = var1;
    }

    @Override
    public boolean equals (Object other) {
      if (!(other instanceof Key)) {
        return false;
      }

      final Key k = (Key)other;
      return (this.fix1 == k.fix1) && (this.fix2 == k.fix2) && (this.var1.equals(k.var1));
    }

    /**
     * HashCode implementation guidelines as per E
     * @return
     */
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + fix1;
      result = 31 * result + (int) (fix2 ^ (fix2 >>> 32));
      result = 31 * result + var1.hashCode();
      return result;
    }
  }

  private static class Value {
    private long sum_m1;
    private int min_m1;
    private int max_m1;
    private long count_m1;

    private long sum_m2;
    private long min_m2;
    private long max_m2;
    private long count_m2;

    private double sum_m3;
    private float min_m3;
    private float max_m3;
    private long count_m3;

    private double sum_m4;
    private double min_m4;
    private double max_m4;
    private long count_m4;

    Value(final long sum_m1, final int min_m1, final int max_m1, final long count_m1,
          final long sum_m2, final long min_m2, final long max_m2, final long count_m2,
          final double sum_m3, final float min_m3, final float max_m3, final long count_m3,
          final double sum_m4, final double min_m4, final double max_m4, final long count_m4) {
      this.sum_m1 = sum_m1;
      this.min_m1 = min_m1;
      this.max_m1 = max_m1;
      this.count_m1 = count_m1;
      this.sum_m2 = sum_m2;
      this.min_m2 = min_m2;
      this.max_m2 = max_m2;
      this.count_m2 = count_m2;
      this.sum_m3 = sum_m3;
      this.min_m3 = min_m3;
      this.max_m3 = max_m3;
      this.count_m3 = count_m3;
      this.sum_m4 = sum_m4;
      this.min_m4 = min_m4;
      this.max_m4 = max_m4;
      this.count_m4 = count_m4;
    }
  }
}
