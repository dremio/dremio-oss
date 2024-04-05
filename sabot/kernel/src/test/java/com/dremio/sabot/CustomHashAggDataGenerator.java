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

/**
 * Used to generate data for testing hash agg spilling functionality. As part of further development
 * (non-contraction etc), this should probably be extended to work with a schema provided by the
 * consumer.
 */
public class CustomHashAggDataGenerator implements Generator {
  private static final FieldType DECIMAL_FIELD_TYPE =
      FieldType.nullable(new ArrowType.Decimal(38, 9, 128));
  private static final ArrowType.Decimal DECIMAL_ARROWTYPE =
      (ArrowType.Decimal) DECIMAL_FIELD_TYPE.getType();
  private static final CompleteType DECIMAL_COMPLETE_TYPE =
      new CompleteType(DECIMAL_ARROWTYPE, new ArrayList<>());

  private static final Field INT_KEY = CompleteType.INT.toField("INT_KEY");
  private static final Field BIGINT_KEY = CompleteType.BIGINT.toField("BIGINT_KEY");
  private static final Field VARCHAR_KEY = CompleteType.VARCHAR.toField("VARCHAR_KEY");
  private static final Field FLOAT_KEY = CompleteType.FLOAT.toField("FLOAT_KEY");
  private static final Field DOUBLE_KEY = CompleteType.DOUBLE.toField("DOUBLE_KEY");
  private static final Field BOOLEAN_KEY = CompleteType.BIT.toField("BOOLEAN_KEY");
  private static final Field DECIMAL_KEY = DECIMAL_COMPLETE_TYPE.toField("DECIMAL_KEY");

  private static final Field INT_MEASURE = CompleteType.INT.toField("INT_MEASURE");
  private static final Field BIGINT_MEASURE = CompleteType.BIGINT.toField("BIGINT_MEASURE");
  private static final Field FLOAT_MEASURE = CompleteType.FLOAT.toField("FLOAT_MEASURE");
  private static final Field DOUBLE_MEASURE = CompleteType.DOUBLE.toField("DOUBLE_MEASURE");
  private static final Field DECIMAL_MEASURE = DECIMAL_COMPLETE_TYPE.toField("DECIMAL_MEASURE");

  /* arrays on heap that will store column values as we generate data for the schema */
  private Integer[] intKeyValues;
  private Long[] bigintKeyValues;
  private String[] varKeyValues;
  private Float[] floatKeyValues;
  private Double[] doubleKeyValues;
  private Boolean[] booleanKeyValues;
  private BigDecimal[] decimalKeyValues;

  private Integer[] intMeasureValues;
  private Long[] bigIntMeasureValues;
  private Float[] floatMeasureValues;
  private Double[] doubleMeasureValues;
  private BigDecimal[] decimalMeasureValues;

  /* vectors in the input container to feed the data into operator */
  private IntVector intKey;
  private BigIntVector bigintKey;
  private VarCharVector varcharKey;
  private Float4Vector floatKey;
  private Float8Vector doubleKey;
  private BitVector booleanKey;
  private DecimalVector decimalKey;

  private IntVector intMeasure;
  private BigIntVector bigintMeasure;
  private Float4Vector floatMeasure;
  private Float8Vector doubleMeasure;
  private DecimalVector decimalMeasure;

  private VectorContainer container;

  private int position;
  private int batches;
  private boolean largeVarChars;

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
  private static final Boolean BOOLEAN_BASEVALUE = false;
  private static final BigDecimal DECIMAL_BASEVALUE = new BigDecimal("10.254567123");
  private static final BigDecimal DECIMAL_INCREMENT = new BigDecimal("1.0");
  private static final int GROUP_INTERVAL_PER_BATCH = 20;

  private int numRows;
  private final Map<Key, Value> aggregatedResults = new HashMap<>();

  private int minLargeVarCharLen = 0;

  private void internalInit(int numRows, BufferAllocator allocator, final boolean largeVarChars) {
    this.numRows = numRows;
    this.batches = numRows / BATCH_SIZE;
    this.largeVarChars = largeVarChars;
    createBigSchemaAndInputContainer(allocator);
    buildInputTableDataAndResultset();
  }

  public CustomHashAggDataGenerator(
      int numRows, BufferAllocator allocator, final boolean largeVarChars) {
    Preconditions.checkState(
        numRows > 0 && numRows % BATCH_SIZE == 0,
        "ERROR: total number of rows should be greater than 0");
    internalInit(numRows, allocator, largeVarChars);
  }

  public CustomHashAggDataGenerator(
      int numRows,
      BufferAllocator allocator,
      final int minVarCharLen /*minimum length of long varchar*/) {
    Preconditions.checkState(
        numRows > 0 && numRows % BATCH_SIZE == 0,
        "ERROR: total number of rows should be greater than 0");

    this.minLargeVarCharLen = minVarCharLen;
    internalInit(numRows, allocator, true);
  }

  private void createBigSchemaAndInputContainer(final BufferAllocator allocator) {
    final BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(INT_KEY)
            .addField(BIGINT_KEY)
            .addField(VARCHAR_KEY)
            .addField(FLOAT_KEY)
            .addField(DOUBLE_KEY)
            .addField(BOOLEAN_KEY)
            .addField(DECIMAL_KEY)
            .addField(INT_MEASURE)
            .addField(BIGINT_MEASURE)
            .addField(FLOAT_MEASURE)
            .addField(DOUBLE_MEASURE)
            .addField(DECIMAL_MEASURE)
            .build();

    container = VectorContainer.create(allocator, schema);

    intKey = container.addOrGet(INT_KEY);
    bigintKey = container.addOrGet(BIGINT_KEY);
    varcharKey = container.addOrGet(VARCHAR_KEY);
    floatKey = container.addOrGet(FLOAT_KEY);
    doubleKey = container.addOrGet(DOUBLE_KEY);
    booleanKey = container.addOrGet(BOOLEAN_KEY);
    decimalKey = container.addOrGet(DECIMAL_KEY);

    intMeasure = container.addOrGet(INT_MEASURE);
    bigintMeasure = container.addOrGet(BIGINT_MEASURE);
    floatMeasure = container.addOrGet(FLOAT_MEASURE);
    doubleMeasure = container.addOrGet(DOUBLE_MEASURE);
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

      /* populate vectors in incoming batch for accumulator column data */
      intMeasure.setSafe(i, intMeasureValues[absoluteRecordIndex]);
      bigintMeasure.setSafe(i, bigIntMeasureValues[absoluteRecordIndex]);
      floatMeasure.setSafe(i, floatMeasureValues[absoluteRecordIndex]);
      doubleMeasure.setSafe(i, doubleMeasureValues[absoluteRecordIndex]);
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
    intKeyValues = new Integer[numRows];
    bigintKeyValues = new Long[numRows];
    varKeyValues = new String[numRows];
    floatKeyValues = new Float[numRows];
    doubleKeyValues = new Double[numRows];
    booleanKeyValues = new Boolean[numRows];
    decimalKeyValues = new BigDecimal[numRows];

    intMeasureValues = new Integer[numRows];
    bigIntMeasureValues = new Long[numRows];
    floatMeasureValues = new Float[numRows];
    doubleMeasureValues = new Double[numRows];
    decimalMeasureValues = new BigDecimal[numRows];

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
          if (largeVarChars && minLargeVarCharLen != 0) {
            varKeyValues[0] = RandomStringUtils.randomAlphabetic(minLargeVarCharLen);
          } else {
            varKeyValues[0] = VARCHAR_BASEVALUE;
          }
          floatKeyValues[0] = FLOAT_BASEVALUE;
          doubleKeyValues[0] = DOUBLE_BASEVALUE;
          booleanKeyValues[0] = BOOLEAN_BASEVALUE;
          decimalKeyValues[0] = DECIMAL_BASEVALUE;

          /* measure columns */
          intMeasureValues[0] = INT_BASEVALUE;
          bigIntMeasureValues[0] = BIGINT_BASEVALUE;
          floatMeasureValues[0] = FLOAT_BASEVALUE;
          doubleMeasureValues[0] = DOUBLE_BASEVALUE;
          decimalMeasureValues[0] = DECIMAL_BASEVALUE;
        } else {
          if (i == pos + (GROUP_INTERVAL_PER_BATCH * GROUP_REPEAT_PER_BATCH)) {
            pos = i;
          }

          /* key columns */
          if (i < pos + GROUP_INTERVAL_PER_BATCH) {
            /* generate unique key column values */
            intKeyValues[i] = intKeyValues[i - 1] + 1;
            bigintKeyValues[i] = bigintKeyValues[i - 1] + 1L;
            if (largeVarChars) {
              /* length of varchars is directly proportional to the desired number of rows we have to generate in the dataset */
              varKeyValues[i] = varKeyValues[i - 1] + String.format("%03d", 1);
            } else {
              varKeyValues[i] = VARCHAR_BASEVALUE + RandomStringUtils.randomAlphabetic(10);
            }
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
          intMeasureValues[i] = intMeasureValues[i - 1] + INT_INCREMENT;
          bigIntMeasureValues[i] = bigIntMeasureValues[i - 1] + BIGINT_INCREMENT;
          floatMeasureValues[i] = floatMeasureValues[i - 1] + FLOAT_INCREMENT;
          doubleMeasureValues[i] = doubleMeasureValues[i - 1] + DOUBLE_INCREMENT;
          decimalMeasureValues[i] = decimalMeasureValues[i - 1].add(DECIMAL_INCREMENT);
        }

        /* compute the hashagg results as we build the data */
        final Key k =
            new Key(
                intKeyValues[i],
                bigintKeyValues[i],
                varKeyValues[i],
                Float.floatToIntBits(floatKeyValues[i]),
                Double.doubleToLongBits(doubleKeyValues[i]),
                booleanKeyValues[i],
                decimalKeyValues[i]);
        Value v = aggregatedResults.get(k);
        if (v == null) {
          v =
              new Value(
                  intMeasureValues[i],
                  intMeasureValues[i],
                  intMeasureValues[i],
                  1,
                  bigIntMeasureValues[i],
                  bigIntMeasureValues[i],
                  bigIntMeasureValues[i],
                  1,
                  floatMeasureValues[i],
                  floatMeasureValues[i],
                  floatMeasureValues[i],
                  1,
                  doubleMeasureValues[i],
                  doubleMeasureValues[i],
                  doubleMeasureValues[i],
                  1,
                  decimalMeasureValues[i],
                  decimalMeasureValues[i],
                  decimalMeasureValues[i],
                  1);
          aggregatedResults.put(k, v);
        } else {
          v.sumInt += intMeasureValues[i];
          v.minInt = (intMeasureValues[i] < v.minInt) ? intMeasureValues[i] : v.minInt;
          v.maxInt = (intMeasureValues[i] > v.maxInt) ? intMeasureValues[i] : v.maxInt;
          v.countInt++;

          v.sumBigInt += bigIntMeasureValues[i];
          v.minBigInt =
              (bigIntMeasureValues[i] < v.minBigInt) ? bigIntMeasureValues[i] : v.minBigInt;
          v.maxBigInt =
              (bigIntMeasureValues[i] > v.maxBigInt) ? bigIntMeasureValues[i] : v.maxBigInt;
          v.countBigInt++;

          v.sumFloat += floatMeasureValues[i];
          v.minFloat = (floatMeasureValues[i] < v.minFloat) ? floatMeasureValues[i] : v.minFloat;
          v.maxFloat = (floatMeasureValues[i] > v.maxFloat) ? floatMeasureValues[i] : v.maxFloat;
          v.countFloat++;

          v.sumDouble += doubleMeasureValues[i];
          v.minDouble =
              (doubleMeasureValues[i] < v.minDouble) ? doubleMeasureValues[i] : v.minDouble;
          v.maxDouble =
              (doubleMeasureValues[i] > v.maxDouble) ? doubleMeasureValues[i] : v.maxDouble;
          v.countDouble++;

          v.sumDecimal = v.sumDecimal.add(decimalMeasureValues[i]);
          v.minDecimal =
              decimalMeasureValues[i].compareTo(v.minDecimal) < 0
                  ? decimalMeasureValues[i]
                  : v.minDecimal;
          v.maxDecimal =
              decimalMeasureValues[i].compareTo(v.minDecimal) > 0
                  ? decimalMeasureValues[i]
                  : v.minDecimal;
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
      rows[row] =
          tr(
              k.intKey,
              k.bigintKey,
              k.varKey,
              Float.intBitsToFloat(k.floatKey),
              Double.longBitsToDouble(k.doubleKey),
              k.booleanKey,
              k.decimalKey,
              v.sumInt,
              v.minInt,
              v.maxInt,
              v.sumBigInt,
              v.minBigInt,
              v.maxBigInt,
              v.sumFloat,
              v.minFloat,
              v.maxFloat,
              v.sumDouble,
              v.minDouble,
              v.maxDouble,
              v.sumDecimal,
              v.minDecimal,
              v.maxDecimal);
      row++;
    }
    return t(
            th(
                "INT_KEY",
                "BIGINT_KEY",
                "VARCHAR_KEY",
                "FLOAT_KEY",
                "DOUBLE_KEY",
                "BOOLEAN_KEY",
                "DECIMAL_KEY",
                "SUM_INT",
                "MIN_INT",
                "MAX_INT",
                "SUM_BIGINT",
                "MIN_BIGINT",
                "MAX_BIGINT",
                "SUM_FLOAT",
                "MIN_FLOAT",
                "MAX_FLOAT",
                "SUM_DOUBLE",
                "MIN_DOUBLE",
                "MAX_DOUBLE",
                "SUM_DECIMAL",
                "MIN_DECIMAL",
                "MAX_DECIMAL"),
            rows)
        .orderInsensitive();
  }

  public Fixtures.Table getExpectedGroupsAndAggregationsWithCount() {
    final Fixtures.DataRow[] rows = new Fixtures.DataRow[GROUPS_PER_BATCH * batches];
    Iterator iterator = aggregatedResults.entrySet().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final Map.Entry<Key, Value> pair = (Map.Entry<Key, Value>) iterator.next();
      final Key k = pair.getKey();
      final Value v = pair.getValue();
      rows[row] =
          tr(
              k.intKey,
              k.bigintKey,
              k.varKey,
              Float.intBitsToFloat(k.floatKey),
              Double.longBitsToDouble(k.doubleKey),
              k.booleanKey,
              k.decimalKey,
              v.sumInt,
              v.minInt,
              v.maxInt,
              v.countInt,
              v.sumBigInt,
              v.minBigInt,
              v.maxBigInt,
              v.countBigInt,
              v.sumFloat,
              v.minFloat,
              v.maxFloat,
              v.countFloat,
              v.sumDouble,
              v.minDouble,
              v.maxDouble,
              v.countDouble,
              v.sumDecimal,
              v.minDecimal,
              v.maxDecimal,
              v.countDecimal);
      row++;
    }
    return t(
            th(
                "INT_KEY",
                "BIGINT_KEY",
                "VARCHAR_KEY",
                "FLOAT_KEY",
                "DOUBLE_KEY",
                "BOOLEAN_KEY",
                "DECIMAL_KEY",
                "SUM_INT",
                "MIN_INT",
                "MAX_INT",
                "COUNT_INT",
                "SUM_BIGINT",
                "MIN_BIGINT",
                "MAX_BIGINT",
                "COUNT_BIGINT",
                "SUM_FLOAT",
                "MIN_FLOAT",
                "MAX_FLOAT",
                "COUNT_FLOAT",
                "SUM_DOUBLE",
                "MIN_DOUBLE",
                "MAX_DOUBLE",
                "COUNT_DOUBLE",
                "SUM_DECIMAL",
                "MIN_DECIMAL",
                "MAX_DECIMAL",
                "COUNT_DECIMAL"),
            rows)
        .orderInsensitive();
  }

  private static final class Key {
    private final int intKey;
    private final long bigintKey;
    private final String varKey;
    private final int floatKey;
    private final long doubleKey;
    private final boolean booleanKey;
    private final BigDecimal decimalKey;

    Key(
        final int intKey,
        final long bigintKey,
        final String varKey,
        final int floatKey,
        final long doubleKey,
        final boolean booleanKey,
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
    public boolean equals(Object other) {
      if (!(other instanceof Key)) {
        return false;
      }

      final Key k = (Key) other;
      return (this.intKey == k.intKey)
          && (this.bigintKey == k.bigintKey)
          && (this.varKey.equals(k.varKey))
          && (this.floatKey == ((Key) other).floatKey)
          && this.doubleKey == ((Key) other).doubleKey
          && (this.booleanKey == ((Key) other).booleanKey)
          && (this.decimalKey.compareTo(((Key) other).decimalKey) == 0);
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
    private int minInt;
    private int maxInt;
    private long countInt;

    private long sumBigInt;
    private long minBigInt;
    private long maxBigInt;
    private long countBigInt;

    private double sumFloat;
    private float minFloat;
    private float maxFloat;
    private long countFloat;

    private double sumDouble;
    private double minDouble;
    private double maxDouble;
    private long countDouble;

    private BigDecimal sumDecimal;
    private BigDecimal minDecimal;
    private BigDecimal maxDecimal;
    private long countDecimal;

    Value(
        final long sumInt,
        final int minInt,
        final int maxInt,
        final long countInt,
        final long sumBigInt,
        final long minBigInt,
        final long maxBigInt,
        final long countBigInt,
        final double sumFloat,
        final float minFloat,
        final float maxFloat,
        final long countFloat,
        final double sumDouble,
        final double minDouble,
        final double maxDouble,
        final long countDouble,
        final BigDecimal sumDecimal,
        final BigDecimal minDecimal,
        final BigDecimal maxDecimal,
        final long countDecimal) {
      this.sumInt = sumInt;
      this.maxInt = maxInt;
      this.minInt = minInt;
      this.countInt = countInt;
      this.sumBigInt = sumBigInt;
      this.maxBigInt = maxBigInt;
      this.minBigInt = minBigInt;
      this.countBigInt = countBigInt;
      this.sumFloat = sumFloat;
      this.maxFloat = maxFloat;
      this.minFloat = minFloat;
      this.countFloat = countFloat;
      this.sumDouble = sumDouble;
      this.maxDouble = maxDouble;
      this.minDouble = minDouble;
      this.countDouble = countDouble;
      this.sumDecimal = sumDecimal;
      this.minDecimal = minDecimal;
      this.maxDecimal = maxDecimal;
      this.countDecimal = countDecimal;
    }
  }
}
