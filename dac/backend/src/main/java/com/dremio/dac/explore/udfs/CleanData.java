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
package com.dremio.dac.explore.udfs;

import static com.dremio.common.util.DateTimes.toMillis;
import static com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.util.Text;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.vector.complex.fn.JsonWriter;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ArrowBuf;

/**
 * UDFs to clean data
 * TODO: more types
 */
public class CleanData {
  private static final Logger logger = LoggerFactory.getLogger(CleanData.class);

  public static DataType initType(NullableVarCharHolder dataType) {
    return DataType.valueOf(toStringFromUTF8(dataType.start, dataType.end, dataType.buffer));
  }

  public static byte[] castToString(FieldReader reader) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      JsonWriter jsonWriter = new JsonWriter(outputStream, true, false);
      jsonWriter.write(reader);
      byte [] bytes = outputStream.toByteArray();
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * See {@link Long#parseLong(String)}
   * @param s
   * @param holder
   * @return
   */
  public static boolean parseLong(String s, NullableBigIntHolder holder) {
    if (s == null) {
      return false;
    }

    long result = 0;
    boolean negative = false;
    int i = 0, len = s.length();
    long limit = -Long.MAX_VALUE;
    long multmin;
    int digit;
    int radix = 10;

    if (len > 0) {
      char firstChar = s.charAt(0);
      if (firstChar < '0') { // Possible leading "+" or "-"
        if (firstChar == '-') {
          negative = true;
          limit = Long.MIN_VALUE;
        } else if (firstChar != '+') {
          return false;
        }

        if (len == 1) {// Cannot have lone "+" or "-"
          return false;
        }
        i++;
      }
      multmin = limit / radix;
      while (i < len) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        digit = Character.digit(s.charAt(i++),radix);
        if (digit < 0) {
          return false;
        }
        if (result < multmin) {
          return false;
        }
        result *= radix;
        if (result < limit + digit) {
          return false;
        }
        result -= digit;
      }
    } else {
      return false;
    }
    holder.value = negative ? result : -result;
    return true;
  }

  public static boolean castToInteger(FieldReader reader, NullableBigIntHolder out) {
    Object o = reader.readObject();
    if (o instanceof Number) {
      out.value = ((Number) o).longValue();
      return true;
    } else if (o instanceof Boolean) {
      out.value = ((Boolean) o).booleanValue() ? 1 : 0;
      return true;
    } else if (o instanceof LocalDateTime) {
      out.value = toMillis((LocalDateTime) o);
      return true;
    } else if (o instanceof Text) {
      try {
        String s = Text.decode(((Text) o).getBytes(), 0, ((Text) o).getLength());
        return parseLong(s, out);
      } catch (CharacterCodingException e) {
        // TODO: is this the best way?
        logger.warn("Can't decode text", e);
        return false;
      }
    } else if (o instanceof byte[]) {
      return false; // TODO
    }
    return false;
  }

  @VisibleForTesting
  static Pattern PATTERN = Pattern.compile( "^([-+]?\\d*)\\.?\\d+([Ee][-+]?\\d+)?$" );

  public static boolean isNumeric(String value) {
    return value != null && PATTERN.matcher(value).matches();
  }

  public static boolean castToFloat(FieldReader reader, NullableFloat8Holder out) {
    Object o = reader.readObject();
    if (o instanceof Number) {
      out.value = ((Number) o).doubleValue();
      return true;
    } else if (o instanceof Boolean) {
      out.value = ((Boolean) o).booleanValue() ? 1 : 0;
      return true;
    } else if (o instanceof LocalDateTime) {
      out.value = toMillis((LocalDateTime) o);
      return true;
    } else if (o instanceof Text) {
      String s;
      try {
        s = Text.decode(((Text) o).getBytes(), 0, ((Text) o).getLength());
        if (!isNumeric(s)) {
          return false;
        }
        out.value = Double.parseDouble(s);
        return true;
      } catch (Exception e) {
        // TODO: is this the best way?
        logger.warn("Can't decode text to FLOAT", e);
        return false;
      }
    } else if (o instanceof byte[]) {
      return false; // TODO
    }
    return false;
  }

  public static boolean castToBoolean(FieldReader reader, NullableBitHolder out) {
    Object o = reader.readObject();
    if (o instanceof Number) {
      out.value = ((Number) o).doubleValue() != 0 ? 1 : 0;
      return true;
    } else if (o instanceof Boolean) {
      out.value = ((Boolean) o).booleanValue() ? 1 : 0;
      return true;
    } else if (o instanceof LocalDateTime) {
      out.value = toMillis((LocalDateTime) o) != 0 ? 1 : 0;
      return true;
    } else if (o instanceof Text) {
      try {
        String s = Text.decode(((Text) o).getBytes(), 0, ((Text) o).getLength());
        if((s == null)
          || (s.length() == 0)
          || ("false".equalsIgnoreCase(s))
          || ("f".equalsIgnoreCase(s))
          || ("0".equals(s))
          || ("0.0".equals(s))) {
          out.value = 0;
          return true;
        } else {
          out.value = 1;
          return true;
        }
      } catch (CharacterCodingException e) {
        logger.warn("Can't decode text", e);
        return false;
      }
    }

    return false;
  }

  /**
   * Is clean data?
   */
  @FunctionTemplate(name = "is_clean_data", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class IsCleanData implements SimpleFunction {

    // TODO: this should be a FieldReader. see DX-1256
    @Param private UnionHolder in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableVarCharHolder dataTypeHolder;
    @Output private NullableBitHolder out;
    @Workspace private com.dremio.dac.proto.model.dataset.DataType expectedDataType;
    @Workspace private NullableBigIntHolder bigIntHolder;
    @Workspace private NullableFloat8Holder floatHolder;

    @Override
    public void setup() {
      expectedDataType = com.dremio.dac.explore.udfs.CleanData.initType(dataTypeHolder);
      bigIntHolder = new NullableBigIntHolder();
      floatHolder = new NullableFloat8Holder();
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 1; // null values are considered clean
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType());
      if (dataType == expectedDataType
          || castWhenPossibleHolder.value == 1) {
        if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.TEXT) {
          // for strings we can always cast
          out.value = 1;
        } else if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.INTEGER) {
          out.value = com.dremio.dac.explore.udfs.CleanData.castToInteger(in.reader, bigIntHolder) ? 1 : 0;
        } else if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.FLOAT) {
          out.value = com.dremio.dac.explore.udfs.CleanData.castToFloat(in.reader, floatHolder) ? 1 : 0;
        } else {
          // TODO: more types
          out.value = 0;
        }
      } else {
        // TODO: more types
        out.value = 0;
      }
    }

  }

  /**
   * Clean data to TEXT
   */
  @FunctionTemplate(name = "clean_data_to_TEXT", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class CleanDataText implements SimpleFunction {

    // TODO: this should be a FieldReader. see DX-1256
    @Param private UnionHolder in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private NullableVarCharHolder defaultValueHolder;
    @Output private NullableVarCharHolder out;
    @Inject private ArrowBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = in.isSet;
      if (out.isSet == 0) {
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType text =
          com.dremio.dac.proto.model.dataset.DataType.TEXT;
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType());
      if (dataType == text
          || castWhenPossibleHolder.value == 1) {
        // for strings we can always cast
        if (in.reader.getMinorType() == org.apache.arrow.vector.types.Types.MinorType.VARCHAR) {
          in.reader.read(out);
        } else {
          byte[] bytes = com.dremio.dac.explore.udfs.CleanData.castToString(in.reader);
          out.start = 0;
          buffer = buffer.reallocIfNeeded(bytes.length);
          out.buffer = buffer;
          out.buffer.setBytes(0, bytes);
          out.end = bytes.length;
          out.isSet = 1;
        }
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.buffer = defaultValueHolder.buffer;
          out.start = defaultValueHolder.start;
          out.end = defaultValueHolder.end;
          out.isSet = 1;
        }
      }
    }

  }

  /**
   * Clean data to INTEGER
   */
  @FunctionTemplate(name = "clean_data_to_INTEGER", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class CleanDataInt implements SimpleFunction {

    // TODO: this should be a FieldReader. see DX-1256
    @Param private UnionHolder in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private NullableBigIntHolder defaultValueHolder;
    @Output private NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.isSet = 0;
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType integer =
          com.dremio.dac.proto.model.dataset.DataType.INTEGER;
      // TODO: check data mode
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType());
      if ((dataType == integer || castWhenPossibleHolder.value == 1)
          && com.dremio.dac.explore.udfs.CleanData.castToInteger(in.reader, out)) {
        out.isSet = 1;
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.value = defaultValueHolder.value;
          out.isSet = 1;
        }
      }
    }
  }

  /**
   * Clean data to FLOAT
   */
  @FunctionTemplate(name = "clean_data_to_FLOAT", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class CleanDataFloat implements SimpleFunction {

    // TODO: this should be a FieldReader. see DX-1256
    @Param private UnionHolder in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private NullableFloat8Holder defaultValueHolder;
    @Output private NullableFloat8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.isSet = 0;
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType floatType =
          com.dremio.dac.proto.model.dataset.DataType.FLOAT;
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType());
      if (((dataType == floatType) || castWhenPossibleHolder.value == 1)
          && com.dremio.dac.explore.udfs.CleanData.castToFloat(in.reader, out)) {
        out.isSet = 1;
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.value = defaultValueHolder.value;
          out.isSet = 1;
        }
      }
    }
  }

  /**
   * Clean data to BOOLEAN
   */
  @FunctionTemplate(name = "clean_data_to_BOOLEAN", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class CleanDataBoolean implements SimpleFunction {

    @Param private UnionHolder in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private NullableBitHolder defaultValueHolder;
    @Output private NullableBitHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (in.isSet == 0) {
        out.isSet = 0;
        return;
      }

      final com.dremio.dac.proto.model.dataset.DataType booleanType =
        com.dremio.dac.proto.model.dataset.DataType.BOOLEAN;
      final com.dremio.dac.proto.model.dataset.DataType dataType =
        com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType());
      if (((dataType == booleanType) || castWhenPossibleHolder.value == 1) &&
          com.dremio.dac.explore.udfs.CleanData.castToBoolean(in.reader, out)) {
        out.isSet = 1;
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.value = defaultValueHolder.value;
          out.isSet = 1;
        }
      }
    }
  }

  /**
   * Get Dremio data type of expression
   */
  @FunctionTemplate(name = "dremio_type_of", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class DremioTypeOf implements SimpleFunction {
    @Param private UnionHolder in;
    @Output private NullableVarCharHolder out;

    @Inject private ArrowBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.isSet = in.isSet;
      if (out.isSet == 0) {
        return;
      }
      byte[] result = com.dremio.dac.explore.DataTypeUtil.getDataType(in.reader.getMinorType()).toString().getBytes();
      out.start = 0;
      buffer = buffer.reallocIfNeeded(result.length);
      out.buffer = buffer;
      out.buffer.setBytes(0, result);
      out.end = result.length;
    }
  }

  /**
   * convert to INTEGER
   */
  @FunctionTemplate(name = "convert_to_INTEGER", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConvertToInt implements SimpleFunction {

    @Param private FieldReader in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private NullableBigIntHolder defaultValueHolder;
    @Output private NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (!in.isSet()) {
        out.isSet = 0;
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType integer =
          com.dremio.dac.proto.model.dataset.DataType.INTEGER;
      // TODO: check data mode
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.getMinorType());
      if ((dataType == integer || castWhenPossibleHolder.value == 1)
          && com.dremio.dac.explore.udfs.CleanData.castToInteger(in, out)) {
        out.isSet = 1;
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.value = defaultValueHolder.value;
          out.isSet = 1;
        }
      }
    }
  }

  /**
   * convert to FLOAT
   */
  @FunctionTemplate(name = "convert_to_FLOAT", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConvertToFloat implements SimpleFunction {

    @Param private FieldReader in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Param(constant=true) private Float8Holder defaultValueHolder;
    @Output private NullableFloat8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (!in.isSet()) {
        out.isSet = 0;
        return;
      }
      final com.dremio.dac.proto.model.dataset.DataType floatType =
          com.dremio.dac.proto.model.dataset.DataType.FLOAT;
      final com.dremio.dac.proto.model.dataset.DataType dataType =
          com.dremio.dac.explore.DataTypeUtil.getDataType(in.getMinorType());
      if ((dataType == floatType || castWhenPossibleHolder.value == 1)
          && com.dremio.dac.explore.udfs.CleanData.castToFloat(in, out)) {
        out.isSet = 1;
      } else {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          out.value = defaultValueHolder.value;
          out.isSet = 1;
        }
      }
    }
  }

  /**
   * Is convertible data?

   */
  @FunctionTemplate(name = "is_convertible_data", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class IsConvertibleData implements SimpleFunction {

    @Param private FieldReader in;
    @Param(constant=true) private NullableIntHolder castWhenPossibleHolder;
    @Param(constant=true) private NullableVarCharHolder dataTypeHolder;
    @Output private NullableBitHolder out;
    @Workspace private com.dremio.dac.proto.model.dataset.DataType expectedDataType;
    @Workspace private NullableBigIntHolder bigIntHolder;
    @Workspace private NullableFloat8Holder floatHolder;

    @Override
    public void setup() {
      expectedDataType = com.dremio.dac.explore.udfs.CleanData.initType(dataTypeHolder);
      bigIntHolder = new NullableBigIntHolder();
      floatHolder = new NullableFloat8Holder();
    }

    @Override
    public void eval() {
        if (!in.isSet()) {
          out.isSet = 0;
          return;
        }
        out.isSet = 1;
        final com.dremio.dac.proto.model.dataset.DataType dataType =
            com.dremio.dac.explore.DataTypeUtil.getDataType(com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType(in.getMinorType()));
        if (dataType == expectedDataType
            || castWhenPossibleHolder.value == 1) {
          if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.TEXT) {
            // for strings we can always cast
            out.value = 1;
          } else if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.INTEGER) {
            out.value = com.dremio.dac.explore.udfs.CleanData.castToInteger(in, bigIntHolder) ? 1 : 0;
          } else if (expectedDataType == com.dremio.dac.proto.model.dataset.DataType.FLOAT) {
            out.value = com.dremio.dac.explore.udfs.CleanData.castToFloat(in, floatHolder) ? 1 : 0;
          } else {
            // TODO: more types
            out.value = 0;
          }
        } else {
          // TODO: more types
          out.value = 0;
        }
    }

  }

}
