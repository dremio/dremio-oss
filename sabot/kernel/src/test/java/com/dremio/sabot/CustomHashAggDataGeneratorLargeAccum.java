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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.RandomStringUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.google.common.base.Preconditions;

/**
 * custom data generator for large number of accumulators in the schema
 */
public class CustomHashAggDataGeneratorLargeAccum implements Generator {
  private static final FieldType decimalFieldType = FieldType.nullable(new ArrowType.Decimal(38, 9, 128));
  private static final ArrowType.Decimal decimalArrowtype = (ArrowType.Decimal)decimalFieldType.getType();
  private static final CompleteType decimalCompleteType = new CompleteType(decimalArrowtype, new ArrayList<>());

  private static final Field INT_KEY = CompleteType.INT.toField("INT_KEY");
  private static final Field BIGINT_KEY = CompleteType.BIGINT.toField("BIGINT_KEY");
  private static final Field VARCHAR_KEY = CompleteType.VARCHAR.toField("VARCHAR_KEY");
  private static final Field FLOAT_KEY = CompleteType.FLOAT.toField("FLOAT_KEY");
  private static final Field DOUBLE_KEY = CompleteType.DOUBLE.toField("DOUBLE_KEY");
  private static final Field BOOLEAN_KEY = CompleteType.BIT.toField("BOOLEAN_KEY");
  private static final Field DECIMAL_KEY = decimalCompleteType.toField("DECIMAL_KEY");

  private static final Field INT_MEASURE = CompleteType.INT.toField("INT_MEASURE");
  /* arrays on heap that will store column values as we generate data for the schema */
  private Integer[] intKeyValues;
  private Long[] bigintKeyValues;
  private String[] varKeyValues;
  private Float[] floatKeyValues;
  private Double[] doubleKeyValues;
  private Boolean[] booleanKeyValues;
  private BigDecimal[] decimalKeyValues;

  private Integer[] intMeasureValues;
  /* vectors in the input container to feed the data into operator */
  private IntVector intKey;
  private BigIntVector bigintKey;
  private VarCharVector varcharKey;
  private Float4Vector floatKey;
  private Float8Vector doubleKey;
  private BitVector booleanKey;
  private DecimalVector decimalKey;

  private IntVector intMeasure;
  private ArrayList<IntVector> accumList = new ArrayList<>();
  private VectorContainer container = null;

  private static final int GROUP_REPEAT_PER_BATCH = 5;
  /* simplify data generation by using a fixed internal batch size */
  private static final int BATCH_SIZE = 1000;
  /* number of unique keys per batch == number of groups per batch */
  private static final int GROUPS_PER_BATCH = BATCH_SIZE / GROUP_REPEAT_PER_BATCH;
  private static final int INT_BASEVALUE = 100;
  private static final int INT_INCREMENT = 10;
  private static final int GROUP_INTERVAL_PER_BATCH = 20;
  private static final long BIGINT_BASEVALUE = 12345678990L;
  private static final long BIGINT_INCREMENT = 25L;
  private static final float FLOAT_BASEVALUE = 100.5F;
  private static final float FLOAT_INCREMENT = 2.5F;
  private static final double DOUBLE_BASEVALUE = 12345.25D;
  private static final double DOUBLE_INCREMENT = 100.25D;
  private static final String VARCHAR_BASEVALUE = "Dremio-3.0";
  private static final Boolean BOOLEAN_BASEVALUE = false;
  private static final BigDecimal DECIMAL_BASEVALUE = new BigDecimal("10.254567123");
  private static final BigDecimal DECIMAL_INCREMENT = new BigDecimal("1.0");

  private int numRows;
  private int numAccum;
  private int position;
  private int batches;
  private final HashMap<Key, Value> aggregatedResults = new HashMap<>();

  public CustomHashAggDataGeneratorLargeAccum(final int numRows, final BufferAllocator allocator, final int numAccum)
  {
    this.numRows = numRows;
    this.numAccum = numAccum;
    this.batches = numRows/BATCH_SIZE;
    this.createBigSchemaAndInputContainer(allocator);
    this.buildInputTableDataAndResultset();
  }

  private void createBigSchemaAndInputContainer(final BufferAllocator allocator) {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    schemaBuilder.addField(INT_KEY)
      .addField(BIGINT_KEY)
      .addField(VARCHAR_KEY)
      .addField(FLOAT_KEY)
      .addField(DOUBLE_KEY)
      .addField(BOOLEAN_KEY)
      .addField(DECIMAL_KEY);
    for (int i = 0; i < this.numAccum; ++i) {
      Field INT_MEASURE = CompleteType.INT.toField("INT_MEASURE_" + i);
      schemaBuilder = schemaBuilder.addField(INT_MEASURE);
    }
    final BatchSchema schema = schemaBuilder.build();

    this.container = VectorContainer.create(allocator, schema);

    intKey = container.addOrGet(INT_KEY);
    bigintKey = container.addOrGet(BIGINT_KEY);
    varcharKey = container.addOrGet(VARCHAR_KEY);
    floatKey = container.addOrGet(FLOAT_KEY);
    doubleKey = container.addOrGet(DOUBLE_KEY);
    booleanKey = container.addOrGet(BOOLEAN_KEY);
    decimalKey = container.addOrGet(DECIMAL_KEY);

    for (int i = 0; i < this.numAccum; ++i) {
      Field INT_MEASURE = CompleteType.INT.toField("INT_MEASURE_" + i);
      IntVector v = container.addOrGet(INT_MEASURE);
      accumList.add(v);
    }
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  @Override
  public int next(int records) {
    if (position == intKeyValues.length) {
      return 0;
    }
    records = Math.min(records, intKeyValues.length - position);
    container.allocateNew();
    for (int i = 0; i < records; i++) {
      final int absoluteRecordIndex = position + i;
      /* populate vectors in incoming batch vectors for key column data */
      intKey.setSafe(i, intKeyValues[absoluteRecordIndex]);
      bigintKey.setSafe(i, bigintKeyValues[absoluteRecordIndex]);
      byte[] valueBytes = varKeyValues[absoluteRecordIndex].getBytes();
      varcharKey.setSafe(i, valueBytes, 0, valueBytes.length);
      floatKey.setSafe(i, floatKeyValues[absoluteRecordIndex]);
      doubleKey.setSafe(i, doubleKeyValues[absoluteRecordIndex]);
      booleanKey.setSafe(i, booleanKeyValues[absoluteRecordIndex] ? 1 : 0);
      decimalKey.setSafe(i, decimalKeyValues[absoluteRecordIndex]);

      // populate vectors in incoming batch for accumulator column data
      for (int j = 0 ; j < this.numAccum; ++j) {
        this.accumList.get(j).setSafe(i, intMeasureValues[absoluteRecordIndex]);
      }
    }
    container.setAllCount(records);
    position += records;
    return records;
  }

  public Fixtures.Table getExpectedGroupsAndAggregations() {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[GROUPS_PER_BATCH * batches];
    Iterator iterator = aggregatedResults.entrySet().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final Map.Entry<CustomHashAggDataGeneratorLargeAccum.Key, CustomHashAggDataGeneratorLargeAccum.Value> pair = (Map.Entry<CustomHashAggDataGeneratorLargeAccum.Key, CustomHashAggDataGeneratorLargeAccum.Value>)iterator.next();
      final CustomHashAggDataGeneratorLargeAccum.Key k = pair.getKey();
      final CustomHashAggDataGeneratorLargeAccum.Value v = pair.getValue();
      List<Object> keys = new ArrayList<>(
        Arrays.asList(k.intKey, k.bigintKey, k.varKey, Float.intBitsToFloat(k.floatKey), Double.longBitsToDouble(k.doubleKey), k.booleanKey, k.decimalKey));
      List<Long> metrics = Collections.nCopies(this.numAccum, v.sumInt);
      keys.addAll(metrics);
      rows[row] = tr(keys.toArray());
      row++;
    }

    String headers[] = new String[this.numAccum + 7];
    headers[0] = "INT_KEY";
    headers[1] = "BIGINT_KEY";
    headers[2] = "VARCHAR_KEY";
    headers[3] = "FLOAT_KEY";
    headers[4] = "DOUBLE_KEY";
    headers[5] = "BOOLEAN_KEY";
    headers[6] = "DECIMAL_KEY";
    int startIdx = 7;
    for (int i = 0; i < this.numAccum; ++i) {
      headers[startIdx] = "SUM_INT";
      ++startIdx;
    }
    return t(th(headers), rows).orderInsensitive();
  }

  private void buildInputTableDataAndResultset() {
    intKeyValues = new Integer[numRows];
    bigintKeyValues = new Long[numRows];
    varKeyValues = new String[numRows];
    floatKeyValues = new Float[numRows];
    doubleKeyValues = new Double[numRows];
    booleanKeyValues = new Boolean[numRows];
    decimalKeyValues = new BigDecimal[numRows];

    intMeasureValues = new Integer[numRows];

    int batch = 0;
    /* build batch by batch */
    while (batch != batches) {
      int startPoint = batch * BATCH_SIZE;
      int pos = startPoint;
      for (int i = startPoint; i < (startPoint + BATCH_SIZE); i++) {
        if (i == 0) {
          /* key columns */
          intKeyValues[0] = INT_BASEVALUE;
          bigintKeyValues[0] = BIGINT_BASEVALUE;
          varKeyValues[0] = VARCHAR_BASEVALUE;
          floatKeyValues[0] = FLOAT_BASEVALUE;
          doubleKeyValues[0] = DOUBLE_BASEVALUE;
          booleanKeyValues[0] = BOOLEAN_BASEVALUE;
          decimalKeyValues[0] = DECIMAL_BASEVALUE;

          /* measure columns */
          intMeasureValues[0] = INT_BASEVALUE;
        } else {
          if (i == pos + (GROUP_INTERVAL_PER_BATCH * GROUP_REPEAT_PER_BATCH)) {
            pos = i;
          }

          /* key columns */
          if (i < pos + GROUP_INTERVAL_PER_BATCH) {
            /* generate unique key column values */
            intKeyValues[i] = intKeyValues[i-1] + 1;
            bigintKeyValues[i] = bigintKeyValues[i-1] + 1L;
            varKeyValues[i] = VARCHAR_BASEVALUE + RandomStringUtils.randomAlphabetic(10);
            floatKeyValues[i] = floatKeyValues[i - 1] + 1.0f;
            doubleKeyValues[i] = doubleKeyValues[i - 1] + 1.0D;
            booleanKeyValues[i] = !booleanKeyValues[i - 1];
            decimalKeyValues[i] = decimalKeyValues[i - 1].add(DECIMAL_INCREMENT);
          } else {
            /* generate duplicate key column values */
            intKeyValues[i] = intKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            bigintKeyValues[i] = bigintKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            varKeyValues[i] = varKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            floatKeyValues[i] = floatKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            doubleKeyValues[i] = doubleKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            booleanKeyValues[i] = booleanKeyValues[i - GROUP_INTERVAL_PER_BATCH];
            decimalKeyValues[i] = decimalKeyValues[i - GROUP_INTERVAL_PER_BATCH];
          }

          /* measure columns */
          intMeasureValues[i] = intMeasureValues[i-1] + INT_INCREMENT;
        }

        /* compute the hashagg results as we build the data */
        final CustomHashAggDataGeneratorLargeAccum.Key k = new CustomHashAggDataGeneratorLargeAccum.Key(intKeyValues[i], bigintKeyValues[i], varKeyValues[i],
          Float.floatToIntBits(floatKeyValues[i]), Double.doubleToLongBits(doubleKeyValues[i]),
          booleanKeyValues[i], decimalKeyValues[i]);
        CustomHashAggDataGeneratorLargeAccum.Value v = aggregatedResults.get(k);
        if (v == null) {
          v = new CustomHashAggDataGeneratorLargeAccum.Value(intMeasureValues[i]);
          aggregatedResults.put(k, v);
        } else {
          v.sumInt += intMeasureValues[i];
        }
      }
      batch++;
    }
    Preconditions.checkArgument(aggregatedResults.size() == (GROUPS_PER_BATCH * batches), "result table built incorrectly");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container);
  }

  private static class Key {
    final int intKey;
    final long bigintKey;
    final String varKey;
    final int floatKey;
    final long doubleKey;
    final boolean booleanKey;
    final BigDecimal decimalKey;

    Key(final int intKey, final long bigintKey, final String varKey,
        final int floatKey, final long doubleKey, final boolean booleanKey,
        final BigDecimal decimalKey) {
      this.intKey = intKey;
      this.bigintKey = bigintKey;
      this.varKey = varKey;
      this.floatKey = floatKey;
      this.doubleKey = doubleKey;
      this.booleanKey = booleanKey;
      this.decimalKey = decimalKey;
    }

    @Override
    public boolean equals (Object other) {
      if (!(other instanceof CustomHashAggDataGeneratorLargeAccum.Key)) {
        return false;
      }

      final CustomHashAggDataGeneratorLargeAccum.Key k = (CustomHashAggDataGeneratorLargeAccum.Key)other;
      return (this.intKey == k.intKey) && (this.bigintKey == k.bigintKey) && (this.varKey.equals(k.varKey))
        && (this.floatKey == ((CustomHashAggDataGeneratorLargeAccum.Key) other).floatKey) && this.doubleKey == ((CustomHashAggDataGeneratorLargeAccum.Key) other).doubleKey
        && (this.booleanKey == ((CustomHashAggDataGeneratorLargeAccum.Key) other).booleanKey) && (this.decimalKey.compareTo(((CustomHashAggDataGeneratorLargeAccum.Key) other).decimalKey) == 0);
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + intKey;
      result = 31 * result + (int) (bigintKey ^ (bigintKey >>> 32));
      result = 31 * result + varKey.hashCode();
      result = 31 * result + floatKey;
      result = 31 * result + (int) (doubleKey ^ (doubleKey >>> 32));
      result = 31 * result + (booleanKey ? 1 : 0);

      return result;
    }
  }

  private static class Value {
    private long sumInt;
    Value(final long in) {
      this.sumInt = in;
    }

    void addTo(long in) {
      this.sumInt += in;
    }

    long get() { return this.sumInt; }
  }
}
