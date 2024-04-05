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
package com.dremio.dac.explore.model;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.DateTimes;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.util.Text;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utility class to read values from {@link FieldReader}s and write in JSON format. Besides
 * converting to JSON, it also
 *
 * <ul>
 *   <li>updates the approximate space the JSON format value took in {@link JsonOutputContext}
 *   <li>truncates the original value to accommodate in available space according to {@link
 *       JsonOutputContext}
 * </ul>
 */
public class DataJsonOutput {
  public static final DateTimeFormatter FORMAT_DATE =
      DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD").withZoneUTC();
  public static final DateTimeFormatter FORMAT_TIMESTAMP =
      DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH24:MI:SS.FFF").withZoneUTC();
  public static final DateTimeFormatter FORMAT_TIME =
      DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SS.FFF").withZoneUTC();
  public static final String DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE =
      "DREMIO_JOB_DATA_NUMBERS_AS_STRINGS";

  /**
   * Lets go with a constant length value for primitive types. Finding the exact length for
   * primitive types (ex. decimal) for every cell is an expensive operation.
   */
  private static final int PRIMITIVE_FIXED_LENGTH = 8;

  /** Null value length. */
  private static final int NULL_VALUE_LENGTH = 4;

  private boolean convertNumbersToStringsEnabled;

  private final class NumberAsStringDisabler implements Closeable {
    private boolean resetOnClose;

    private NumberAsStringDisabler() {
      // if the feature was already disabled. we should not enable it, as there is some parent call
      // that should handle this
      resetOnClose = convertNumbersToStringsEnabled;
      convertNumbersToStringsEnabled = false;
    }

    @Override
    public void close() {
      if (resetOnClose) {
        convertNumbersToStringsEnabled = true;
      }
    }
  }

  /**
   * Interface that implements writing a specific type value in UnionVector. Instead of having a
   * switch on current value type, create objects which can be found in hash map {@link
   * #UNION_FIELD_WRITERS} and executed to write the current value.
   */
  private interface UnionFieldWriter {
    void write(DataJsonOutput output, FieldReader reader, JsonOutputContext context)
        throws IOException;
  }

  public static final ObjectWriter setNumbersAsStrings(ObjectWriter writer, boolean isEnabled) {
    return writer.withAttribute(
        DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE, isEnabled);
  }

  public static final boolean isNumberAsString(DatabindContext context) {
    Object attr = context.getAttribute(DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE);
    return attr instanceof Boolean && ((Boolean) attr).booleanValue();
  }

  private static final Map<MinorType, UnionFieldWriter> UNION_FIELD_WRITERS =
      ImmutableMap.<MinorType, UnionFieldWriter>builder()
          .put(
              MinorType.FLOAT4,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeFloat(reader, context);
                }
              })
          .put(
              MinorType.FLOAT8,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeDouble(reader, context);
                }
              })
          .put(
              MinorType.INT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeInt(reader, context);
                }
              })
          .put(
              MinorType.SMALLINT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeSmallInt(reader, context);
                }
              })
          .put(
              MinorType.TINYINT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeTinyInt(reader, context);
                }
              })
          .put(
              MinorType.BIGINT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeBigInt(reader, context);
                }
              })
          .put(
              MinorType.BIT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeBit(reader, context);
                }
              })
          .put(
              MinorType.DATE,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeDate(reader, context);
                }
              })
          .put(
              MinorType.TIME,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeTime(reader, context);
                }
              })
          .put(
              MinorType.TIMESTAMP,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeTimeStampMilli(reader, context);
                }
              })
          .put(
              MinorType.INTERVALDAY,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeIntervalDay(reader, context);
                }
              })
          .put(
              MinorType.INTERVALYEAR,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeIntervalYear(reader, context);
                }
              })
          .put(
              MinorType.DECIMAL,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeDecimal(reader, context);
                }
              })
          .put(
              MinorType.LIST,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeList(reader, context);
                }
              })
          .put(
              MinorType.STRUCT,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeStruct(reader, context);
                }
              })
          .put(
              MinorType.MAP,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeMap(reader, context);
                }
              })
          .put(
              MinorType.NULL,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeNull(context);
                }
              })
          .put(
              MinorType.VARCHAR,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeVarChar(reader, context);
                }
              })
          .put(
              MinorType.VAR16CHAR,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeVar16Char(reader, context);
                }
              })
          .put(
              MinorType.VARBINARY,
              new UnionFieldWriter() {
                @Override
                public void write(
                    DataJsonOutput output, FieldReader reader, JsonOutputContext context)
                    throws IOException {
                  output.writeVarBinary(reader, context);
                }
              })
          .build();

  private final JsonGenerator gen;

  DataJsonOutput(JsonGenerator gen, boolean convertNumbersToStrings) {
    this.gen = checkNotNull(gen);
    this.convertNumbersToStringsEnabled = convertNumbersToStrings;
  }

  public void writeStartArray() throws IOException {
    gen.writeStartArray();
  }

  public void writeEndArray() throws IOException {
    gen.writeEndArray();
  }

  public void writeStartObject() throws IOException {
    gen.writeStartObject();
  }

  public void writeEndObject() throws IOException {
    gen.writeEndObject();
  }

  public void writeFieldName(String name) throws IOException {
    gen.writeFieldName(name);
  }

  public void writeVarChar(String value) throws IOException {
    gen.writeString(value);
  }

  public void writeBoolean(boolean value) throws IOException {
    gen.writeBoolean(value);
  }

  public void writeDecimal(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final BigDecimal value = reader.readBigDecimal();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toPlainString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeTinyInt(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Byte value = reader.readByte();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeSmallInt(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Short value = reader.readShort();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeInt(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Integer value = reader.readInteger();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeBigInt(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Long value = reader.readLong();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeFloat(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Float value = reader.readFloat();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeDouble(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      final Double value = reader.readDouble();
      if (convertNumbersToStringsEnabled) {
        gen.writeString(value.toString());
      } else {
        gen.writeNumber(value);
      }
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeVarChar(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      // NB: For UnionReader(s), reader.isSet() checks if the reader has a type set, not if the data
      // itself is null
      final Text text = reader.readText();
      if (text != null) {
        String value = text.toString();
        if (value.length() > context.getRemaining()) {
          // Truncate the string if the space is limited
          value = value.substring(0, context.getRemaining());
          context.setTruncated();
        }
        writeVarChar(value);
        context.used(value.length());
        return;
      }
    }
    // Either the type or the value is not set
    writeNull(context);
  }

  public void writeVar16Char(FieldReader reader, JsonOutputContext context) throws IOException {
    writeVarChar(reader, context);
  }

  public void writeVarBinary(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      byte[] value = reader.readByteArray();
      int len = value.length;
      if (len > context.getRemaining()) {
        // Truncate the byte array if the space is limited
        len = context.getRemaining();
        context.setTruncated();
      }
      gen.writeBinary(value, 0, len);
      context.used(len);
    } else {
      writeNull(context);
    }
  }

  public void writeBit(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      gen.writeBoolean(reader.readBoolean());
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeDateMilli(FieldReader reader, JsonOutputContext context) throws IOException {
    writeDate(reader, context);
  }

  public void writeDate(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      gen.writeString(
          FORMAT_DATE.print(DateTimes.toMillis(JodaDateUtility.readLocalDateTime(reader))));
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeTimeMilli(FieldReader reader, JsonOutputContext context) throws IOException {
    writeTime(reader, context);
  }

  public void writeTime(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      gen.writeString(
          FORMAT_TIME.print(DateTimes.toMillis(JodaDateUtility.readLocalDateTime(reader))));
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeTimeStampMilli(FieldReader reader, JsonOutputContext context)
      throws IOException {
    if (reader.isSet()) {
      gen.writeString(
          FORMAT_TIMESTAMP.print(DateTimes.toMillis(JodaDateUtility.readLocalDateTime(reader))));
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeIntervalYear(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      gen.writeString(
          DremioStringUtils.formatIntervalYear(JodaDateUtility.readPeriodInYears(reader)));
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeIntervalDay(FieldReader reader, JsonOutputContext context) throws IOException {
    if (reader.isSet()) {
      gen.writeString(
          DremioStringUtils.formatIntervalDay(JodaDateUtility.readPeriodInDays(reader)));
      context.used(PRIMITIVE_FIXED_LENGTH);
    } else {
      writeNull(context);
    }
  }

  public void writeNull(JsonOutputContext context) throws IOException {
    gen.writeNull();
    context.used(NULL_VALUE_LENGTH);
  }

  public void writeUnion(FieldReader reader, JsonOutputContext context) throws IOException {
    UNION_FIELD_WRITERS
        .get(getMinorTypeFromArrowMinorType(reader.getMinorType()))
        .write(this, reader, context);
  }

  public void writeStruct(FieldReader reader, JsonOutputContext context) throws IOException {
    try (final NumberAsStringDisabler disabler = new NumberAsStringDisabler()) {
      if (reader.isSet()) {
        writeStartObject();
        for (String name : reader) {
          FieldReader childReader = reader.reader(name);
          if (childReader.isSet()) {
            if (!context.okToWrite()) {
              context.setTruncated();
              break;
            }
            writeFieldName(name);
            writeUnion(childReader, context);
          }
        }
        writeEndObject();
      } else {
        writeNull(context);
      }
    }
  }

  public void writeList(FieldReader reader, JsonOutputContext context) throws IOException {
    try (final NumberAsStringDisabler disabler = new NumberAsStringDisabler()) {
      if (reader.isSet()) {
        writeStartArray();
        while (reader.next()) {
          if (!context.okToWrite()) {
            context.setTruncated();
            break;
          }
          writeUnion(reader.reader(), context);
        }
        writeEndArray();
      } else {
        writeNull(context);
      }
    }
  }

  public void writeMap(FieldReader reader, JsonOutputContext context) throws IOException {
    try (final NumberAsStringDisabler disabler = new NumberAsStringDisabler()) {
      if (reader.isSet()) {
        writeStartObject();
        UnionMapReader mapReader = (UnionMapReader) reader;
        while (reader.next()) {
          if (!context.okToWrite()) {
            context.setTruncated();
            break;
          }
          writeFieldName(getFieldNameFromMapKey(mapReader.key()));
          writeUnion(mapReader.value(), context);
        }
        writeEndObject();
      } else {
        writeNull(context);
      }
    }
  }

  private String getFieldNameFromMapKey(FieldReader keyReader) {
    final MinorType mt = getMinorTypeFromArrowMinorType(keyReader.getMinorType());
    switch (mt) {
      case FLOAT4:
        return keyReader.readFloat().toString();
      case FLOAT8:
        return keyReader.readDouble().toString();
      case INT:
        return keyReader.readInteger().toString();
      case BIGINT:
        return keyReader.readLong().toString();
      case BIT:
        return keyReader.readBoolean().toString();
      case DATE:
        return keyReader.readLocalDateTime().toLocalDate().toString();
      case TIME:
        return keyReader.readLocalDateTime().toLocalTime().toString();
      case TIMESTAMP:
        return keyReader.readLocalDateTime().toString();
      case DECIMAL:
        return keyReader.readBigDecimal().toPlainString();
      case VARCHAR:
        return keyReader.readText().toString();
      case VARBINARY:
        return Base64Variants.getDefaultVariant().encode(keyReader.readByteArray());
      case LIST:
      case STRUCT:
      case MAP:
      case NULL:
      default:
        throw new UnsupportedOperationException("unsupported type for a map key " + mt.name());
    }
  }
}
