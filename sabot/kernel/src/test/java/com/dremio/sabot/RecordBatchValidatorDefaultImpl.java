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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.expression.Describer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.google.common.base.Preconditions;

import de.vandermeer.asciitable.v2.V2_AsciiTable;
import de.vandermeer.asciitable.v2.render.V2_AsciiTableRenderer;
import de.vandermeer.asciitable.v2.render.WidthAbsoluteEven;
import de.vandermeer.asciitable.v2.themes.V2_E_TableThemes;

public class RecordBatchValidatorDefaultImpl implements RecordBatchValidator{

  protected final RecordSet recordSet;

  public RecordBatchValidatorDefaultImpl(RecordSet recordSet) {
    this.recordSet = recordSet;
  }

  @Override
  public void checkValid(List<RecordBatchData> actualBatches) {
    assertThat(actualBatches.size()).as("Batch count").isGreaterThan(0);

    int actualCount = actualBatches.stream()
      .map(RecordBatchData::getRecordCount)
      .reduce(0, Integer::sum);
    int expectedCount = Arrays.stream(recordSet.getBatches())
      .map(b -> b.records.length)
      .reduce(0, Integer::sum);

    V2_AsciiTable output = new V2_AsciiTable();
    boolean schemasMatch = compareSchemas(recordSet.getSchema(), actualBatches.get(0).getSchema(), output);
    // only compare records if schemas match
    boolean recordsMatch = schemasMatch && compareAllRecords(recordSet.getBatches(), actualBatches, output);

    V2_AsciiTableRenderer rend = new V2_AsciiTableRenderer();
    rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
    rend.setWidth(new WidthAbsoluteEven(300));

    assertThat(schemasMatch)
      .as(() -> "\n" + rend.render(output) +
        "\nSchemas do not match - fields displayed as [ actual (expected) ]")
      .isTrue();
    assertThat(actualCount)
      .as(() -> "\n" + rend.render(output)  +
        "\nRecord counts do not match - column values displayed as [ actual (expected) ]")
      .isEqualTo(expectedCount);
    assertThat(recordsMatch)
      .as(() -> "\n" + rend.render(output) +
        "\nRecords do not match - column values displayed as [ actual (expected) ]")
      .isTrue();
  }

  private boolean compareSchemas(BatchSchema expectedSchema, BatchSchema actualSchema, V2_AsciiTable output) {
    // compare schemas & output header row
    boolean matches = true;
    int maxFieldCount = Math.max(expectedSchema.getFieldCount(), actualSchema.getFieldCount());
    Object[] outputValues = new Object[maxFieldCount];
    for (int c = 0; c < maxFieldCount; c++) {
      Field expectedField = expectedSchema.getColumn(c);
      Field actualField = actualSchema.getColumn(c);
      boolean columnMatches = Objects.equals(expectedField, actualField);
      if (columnMatches) {
        outputValues[c] = Describer.describe(actualField);
      } else {
        matches = false;
        outputValues[c] = String.format("%s (%s)",
          actualField == null ? "-" : Describer.describe(actualField),
          expectedField == null ? "-" : Describer.describe(expectedField));
      }
    }

    output.addRule();
    output.addRow(outputValues);
    output.addRule();

    return matches;
  }

  private boolean compareAllRecords(RecordSet.Batch[] expected, List<RecordBatchData> actual, V2_AsciiTable output) {
    int actualBatchIndex = 0;
    int expectedBatchIndex = 0;
    int actualIndex = 0;
    int expectedIndex = 0;
    List<ValueVector> actualVectors = actual.get(actualBatchIndex).getVectors();
    boolean matches = true;
    Object[] outputValues = new Object[recordSet.getSchema().getFieldCount()];

    // compare records until either actual or expected records are fully iterated
    while (actualBatchIndex < actual.size() && expectedBatchIndex < expected.length) {
      while (actualBatchIndex < actual.size() && actualIndex == actual.get(actualBatchIndex).getRecordCount()) {
        actualBatchIndex++;
        actualIndex = 0;
        if (actualBatchIndex < actual.size()) {
          actualVectors = actual.get(actualBatchIndex).getVectors();
        }
      }

      while (expectedBatchIndex < expected.length && expectedIndex == expected[expectedBatchIndex].records.length) {
        expectedBatchIndex++;
        expectedIndex = 0;
      }

      if (actualBatchIndex < actual.size() && expectedBatchIndex < expected.length) {
        boolean recordsMatch =
          compareRecord(expected[expectedBatchIndex].records[expectedIndex], actualVectors, actualIndex, output);
        matches = matches && recordsMatch;

        actualIndex++;
        expectedIndex++;
      }
    }

    // output any additional actual records
    while (actualBatchIndex < actual.size()) {
      while (actualBatchIndex < actual.size() && actualIndex == actual.get(actualBatchIndex).getRecordCount()) {
        actualBatchIndex++;
        actualIndex = 0;
        if (actualBatchIndex < actual.size()) {
          actualVectors = actual.get(actualBatchIndex).getVectors();
        }
      }

      if (actualBatchIndex < actual.size()) {
        for (int c = 0; c < actualVectors.size(); c++) {
          FieldReader reader = actualVectors.get(c).getReader();
          reader.setPosition(actualIndex);
          outputValues[c] = String.format("%s (-)", actualToString(reader));
        }

        output.addRow(outputValues);
        output.addRule();

        actualIndex++;
      }
    }

    // output any additional expected records
    while (expectedBatchIndex < expected.length) {
      while (expectedBatchIndex < expected.length && expectedIndex == expected[expectedBatchIndex].records.length) {
        expectedBatchIndex++;
        expectedIndex = 0;
      }

      if (expectedBatchIndex < expected.length) {
        RecordSet.Record record = expected[expectedBatchIndex].records[expectedIndex];
        for (int c = 0; c < record.values.length; c++) {
          outputValues[c] = String.format("- (%s)", expectedToString(record.values[c]));
        }

        output.addRow(outputValues);
        output.addRule();

        expectedIndex++;
      }
    }

    return matches;
  }

  protected boolean compareRecord(RecordSet.Record expected, List<ValueVector> actual, int actualIndex, V2_AsciiTable output) {
    assertThat(actual.size()).isEqualTo(expected.values.length);
    boolean matches = true;
    Object[] outputValues = new Object[actual.size()];
    for (int c = 0; c < actual.size(); c++) {
      FieldReader reader = actual.get(c).getReader();
      reader.setPosition(actualIndex);
      boolean columnMatches = compareValue(expected.values[c], reader);
      reader.setPosition(actualIndex);
      String actualText = actualToString(reader);
      if (columnMatches) {
        outputValues[c] = actualText;
      } else {
        matches = false;
        outputValues[c] = String.format("%s (%s)", actualText, expectedToString(expected.values[c]));
      }
    }

    output.addRow(outputValues);
    output.addRule();

    return matches;
  }

  protected static String actualToString(FieldReader actual) {
    StringBuilder builder = new StringBuilder();
    actualToString(actual, builder);
    return builder.toString();
  }

  private static void actualToString(FieldReader actual, StringBuilder builder) {
    if (!actual.isSet()) {
      builder.append("null");
      return;
    }

    boolean first = true;
    switch (actual.getMinorType()) {
      case STRUCT:
        builder.append("{ ");
        for (Field child : actual.getField().getChildren()) {
          if (!first) {
            builder.append(", ");
          }
          actualToString(actual.reader(child.getName()), builder);
          first = false;
        }
        builder.append(" }");
        break;
      case LIST:
        builder.append("[ ");
        while (actual.next()) {
          if (!first) {
            builder.append(", ");
          }
          actualToString(actual.reader(), builder);
          first = false;
        }
        builder.append(" ]");
        break;
      case BIGINT:
        builder.append(actual.readObject());
        builder.append("L");
        break;
      case FLOAT4:
        builder.append(actual.readObject());
        builder.append("f");
        break;
      case VARCHAR:
        builder.append("\"");
        builder.append(actual.readObject());
        builder.append("\"");
        break;
      case BIT:
      case INT:
      case FLOAT8:
      case TIMESTAMPMILLI:
      case DATEMILLI:
      case TIMEMILLI:
      case VARBINARY:
      case INTERVALDAY:
      case INTERVALYEAR:
      case DECIMAL:
        Object actualValue = actual.readObject();
        builder.append(actualValue);
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported type %s for field %s", actual.getMinorType(), actual.getField().getName()));
    }
  }

  protected boolean compareValue(Object expectedValue, FieldReader actual) {
    if (!actual.isSet() || expectedValue == null) {
      return !actual.isSet() && expectedValue == null;
    }

    switch (actual.getMinorType()) {
      case STRUCT:
        return compareStruct(expectedValue, actual);
      case LIST:
        return compareList(expectedValue, actual);
      case BIT:
      case INT:
      case BIGINT:
      case TIMESTAMPMILLI:
      case DATEMILLI:
      case TIMEMILLI:
      case VARCHAR:
      case INTERVALDAY:
      case INTERVALYEAR:
      case DECIMAL:
        return Objects.equals(actual.readObject(), normalizeExpected(expectedValue));
      case VARBINARY:
        return expectedValue instanceof byte[] && Arrays.equals((byte[]) actual.readObject(), (byte[]) expectedValue);
      case FLOAT4:
        return compareFloat((Float) actual.readObject(),
          expectedValue instanceof Float ? (Float) expectedValue : null);
      case FLOAT8:
        return compareDouble((Double) actual.readObject(),
          expectedValue instanceof Double ? (Double) expectedValue : null);
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported type %s for field %s", actual.getMinorType(), actual.getField().getName()));
    }
  }

  private boolean compareStruct(Object expected, FieldReader actual) {
    if (!(expected instanceof RecordSet.Tuple)) {
      return false;
    }

    Object[] values = ((RecordSet.Tuple) expected).values;
    List<Field> fields = actual.getField().getChildren();
    Preconditions.checkState(values.length == fields.size(),
      "Provided struct value has incorrect field count - expected %s fields: %s", fields.size(),
      expectedToString(expected));
    boolean matches = true;
    for (int i = 0; matches && i < values.length; i++) {
      matches = compareValue(values[i], actual.reader(fields.get(i).getName()));
    }

    return matches;
  }

  private boolean compareList(Object expected, FieldReader actual) {
    if (!(expected instanceof List<?>)) {
      return false;
    }

    List<?> values = (List<?>) expected;
    int i = 0;
    boolean matches = actual.size() == values.size();
    while (matches && actual.next()) {
      matches = compareValue(values.get(i++), actual.reader());
    }

    return matches;
  }

  private boolean compareFloat(Float actual, Float expected) {
    if (actual == null) {
      return expected == null;
    }

    if (actual.isNaN()) {
      return expected.isNaN();
    }

    if (actual.isInfinite()) {
      return expected.isInfinite();
    }

    if ((actual + expected) / 2 != 0) {
      return Math.abs(actual - expected) / Math.abs((actual + expected) / 2) < 1.0E-6;
    } else {
      return actual == 0;
    }
  }

  private boolean compareDouble(Double actual, Double expected) {
    if (actual == null) {
      return expected == null;
    }

    if (actual.isNaN()) {
      return expected.isNaN();
    }

    if (actual.isInfinite()) {
      return expected.isInfinite();
    }

    if ((actual + expected) / 2 != 0) {
      return Math.abs(actual - expected) / Math.abs((actual + expected) / 2) < 1.0E-6;
    } else {
      return actual == 0;
    }
  }

  protected static Object normalizeExpected(Object expected) {
    if (expected instanceof LocalDate) {
      return ((LocalDate) expected).atStartOfDay();
    } else if (expected instanceof LocalTime) {
      return ((LocalTime) expected).atDate(LocalDate.of(1970, 1, 1));
    } else if (expected instanceof String) {
      return new Text((String) expected);
    } else if (expected instanceof Period) {
      return Period.ofMonths((int) ((Period) expected).toTotalMonths());
    }

    return expected;
  }

  public static String expectedToString(Object expected) {
    StringBuilder builder = new StringBuilder();
    expectedToString(expected, builder);
    return builder.toString();
  }

  private static void expectedToString(Object expected, StringBuilder builder) {
    boolean first = true;
    if (expected == null) {
      builder.append("null");
    } else if (expected instanceof RecordSet.Tuple) {
      builder.append("{ ");
      for (Object v : ((RecordSet.Tuple) expected).values) {
        if (!first) {
          builder.append(", ");
        }
        expectedToString(v, builder);
        first = false;
      }
      builder.append(" }");
    } else if (expected instanceof List<?>) {
      builder.append("[ ");
      for (Object v : (List<?>) expected) {
        if (!first) {
          builder.append(", ");
        }
        expectedToString(v, builder);
        first = false;
      }
      builder.append(" ]");
    } else if (expected instanceof Long) {
      builder.append(expected);
      builder.append("L");
    } else if (expected instanceof Float) {
      builder.append(expected);
      builder.append("f");
    } else if (expected instanceof String) {
      builder.append("\"");
      builder.append(expected);
      builder.append("\"");
    } else {
      builder.append(normalizeExpected(expected));
    }
  }
}
