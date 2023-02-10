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
package com.dremio.exec.store.easy;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.expr.fn.impl.DecimalFunctions;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.store.sys.TimezoneAbbreviations;

public class EasyFormatUtils {

  public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-DD";
  public static final String DEFAULT_TIME_FORMAT = "HH24:MI:SS.FFF";
  public static final String DEFAULT_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FFF";
  public static Object getValue(Field field, String varcharValue, ExtendedFormatOptions extendedFormatOptions) {
    //In case of CSV reader. data value should not have any impact of trim space. so default value is false for CSV reader
    varcharValue = applyStringTransformations(varcharValue, extendedFormatOptions, false);
    if(varcharValue == null) {
      return null;
    }
    ArrowType type = field.getType();
    try {
      if (CompleteType.BIT.getType().equals(type)) {
        return TextBooleanFunction.apply(varcharValue);
      } else if (CompleteType.INT.getType().equals(type)) {
        return Integer.valueOf(varcharValue);
      } else if (CompleteType.BIGINT.getType().equals(type)) {
        return Long.valueOf(varcharValue);
      } else if (CompleteType.FLOAT.getType().equals(type)) {
        return Float.valueOf(varcharValue);
      } else if (CompleteType.DOUBLE.getType().equals(type)) {
        return Double.valueOf(varcharValue);
      } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
        return getBigDecimalValue(type, varcharValue);
      } else if (CompleteType.VARCHAR.getType().equals(type)) {
        return varcharValue;
      } else if (CompleteType.DATE.getType().equals(type)) {
        final long dateInMilliseconds = getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
        return getDaysFromEpoch(dateInMilliseconds);
      } else if (CompleteType.TIME.getType().equals(type)) {
        return 1000 * getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
      } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
        return 1000 * getDateTimeValueAsUnixTimestamp(varcharValue, type, extendedFormatOptions);
      } else {
        throw new RuntimeException("Unsupported data type : " + type);
      }
    } catch (IllegalArgumentException e) {
      throw handleExceptionDuringCoercion(varcharValue, type, field.getName(), e);
    }
  }

  public static RuntimeException handleExceptionDuringCoercion(String varcharValue, ArrowType type, String fieldName, IllegalArgumentException caughtException) {
    StringBuilder errorMessageBuilder = new StringBuilder();
    if(fieldName != null && !fieldName.isEmpty()) {
      errorMessageBuilder.append(String.format("While processing field \"%s\". ", fieldName));
    }
    if (CompleteType.BIT.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to BOOLEAN. Reason: ", varcharValue));
      errorMessageBuilder.append(caughtException.getMessage());
    } else if (CompleteType.INT.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to INT.", varcharValue));
    } else if (CompleteType.BIGINT.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to BIGINT.", varcharValue));
    } else if (CompleteType.FLOAT.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to FLOAT.", varcharValue));
    } else if (CompleteType.DOUBLE.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to DOUBLE.", varcharValue));
    } else if (ArrowType.ArrowTypeID.Decimal.equals(type.getTypeID())) {
      int precision = ((ArrowType.Decimal) type).getPrecision();
      int scale = ((ArrowType.Decimal) type).getScale();
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to DECIMAL(%s,%s).", varcharValue, precision, scale));
      if(caughtException.getMessage() != null) {
        // If overflow was the reason why we errored out, that reason will be added here.
        errorMessageBuilder.append(" Reason: " + caughtException.getMessage());
      }
    } else if (CompleteType.DATE.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to DATE. Reason: ", varcharValue));
      errorMessageBuilder.append(caughtException.getMessage());
    } else if (CompleteType.TIME.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to TIME. Reason: ", varcharValue));
      errorMessageBuilder.append(caughtException.getMessage());
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      errorMessageBuilder.append(String.format("Could not convert \"%s\" to TIMESTAMP. Reason: ", varcharValue));
      errorMessageBuilder.append(caughtException.getMessage());
    } else {
      throw new RuntimeException("Unsupported data type : " + type);
    }
    return new RuntimeException(errorMessageBuilder.toString());
  }

  public static String applyStringTransformations(String varcharValue, ExtendedFormatOptions extendedFormatOptions, Boolean trimSpace) {
    if (extendedFormatOptions == null) {
      return varcharValue;
    }
    final Boolean emptyAsNull = extendedFormatOptions.getEmptyAsNull();
    final List<String> nullIfExpressions = extendedFormatOptions.getNullIfExpressions();

    if (trimSpace != null && trimSpace) {
      varcharValue = varcharValue.trim();
    }

    if (emptyAsNull != null && emptyAsNull && varcharValue.isEmpty()) {
      return null;
    }

    if (nullIfExpressions != null && nullIfExpressions.contains(varcharValue)) {
      return null;
    }

    return varcharValue;
  }

  public static Function<String, Boolean> TextBooleanFunction = s -> {
    if (("true").equalsIgnoreCase(s) || ("1").equalsIgnoreCase(s)) {
      return true;
    } else if (("false").equalsIgnoreCase(s) || ("0").equalsIgnoreCase(s)) {
      return false;
    }
    throw new IllegalArgumentException("Unsupported boolean type value : " + s);
  };

  public static Function<String, Integer> JsonBooleanFunction = s -> {
    if (("true").equalsIgnoreCase(s)) {
      return 1;
    } else if (("false").equalsIgnoreCase(s)) {
      return 0;
    }
    throw new IllegalArgumentException("Unsupported boolean type value : " + s);
  };

  public static BigDecimal getBigDecimalValue(ArrowType type, String varcharValue) {
    BigDecimal bd = new BigDecimal(varcharValue).setScale(((ArrowType.Decimal) type).getScale(), java.math.RoundingMode.HALF_UP);
    // Decimal value will be 0 if there is precision overflow.
    // This is similar to DecimalFunctions:CastDecimalDecimal
    if (DecimalFunctions.checkOverflow(bd, ((ArrowType.Decimal) type).getPrecision())) {
      throw new IllegalArgumentException("Decimal value overflow: " + varcharValue);
    }
    return bd;
  }

  /**
   * Process the given date/time/timestamp value and calculate it's value as a Unix timestamp.
   * @param rawInput The date/time/timestamp value represented as a string.
   * @param type The field type (DATE/TIME/TIMESTAMP).
   * @param extendedFormatOptions Contains all the format options specified by the user in their query.
   * @return Unix timestamp equivalent for the given input, represented as a Long type object.
   */
  public static Long getDateTimeValueAsUnixTimestamp(String rawInput, ArrowType type, ExtendedFormatOptions extendedFormatOptions) {
    if(rawInput == null) {
      return null;
    }

    // First, get the format string from the format options.
    String rawFormat = getDateTimeFormat(type, extendedFormatOptions);

    // Check if the format that we receive from the user ends with a timezone abbreviation. If yes, modify:
    // 1. The format string to specify that we will be using the timezone offset instead of the abbreviation.
    // 2. The input where we replace the timezone abbreviation with its corresponding offset.
    String formatString = isTimezonePresentInFormat(rawFormat) ? replaceTimeZoneWithOffsetFormatString(rawFormat) : rawFormat;
    String inputString = isTimezonePresentInFormat(rawFormat) ? replaceTimeZoneWithOffsetInputString(rawInput) : rawInput;

    final DateTimeFormatter formatter = DateFunctionsUtils.getSQLFormatterForFormatString(formatString);
    if(CompleteType.DATE.getType().equals(type)) {
      return Long.valueOf(DateFunctionsUtils.formatDateMilli(inputString, formatter));
    } else if (CompleteType.TIME.getType().equals(type)) {
      return Long.valueOf(DateFunctionsUtils.formatTime(inputString, formatter));
    } else if (CompleteType.TIMESTAMP.getType().equals(type)) {
      return Long.valueOf(DateFunctionsUtils.formatTimeStampMilli(inputString, formatter));
    } else {
      throw new RuntimeException("Unsupported data type : " + type);
    }
  }

  /**
   * @param dateInMilliseconds The Unix timestamp for the date we were given.
   * @return The number of days passed from Jan 01, 1970 (Unix Epoch) for the given date.
   */
  private static int getDaysFromEpoch(Long dateInMilliseconds) {

    final DateTime epoch = new DateTime(0);
    final DateTime inputDate = new DateTime(dateInMilliseconds);
    return Days.daysBetween(epoch, inputDate).getDays();
  }

  private static String getDateTimeFormat(ArrowType fieldType, ExtendedFormatOptions extendedFormatOptions) {
    String formatString = "";
    if(CompleteType.DATE.getType().equals(fieldType)) {
      formatString = extendedFormatOptions.getDateFormat() == null ? DEFAULT_DATE_FORMAT : extendedFormatOptions.getDateFormat();
    } else if (CompleteType.TIME.getType().equals(fieldType)) {
      formatString = extendedFormatOptions.getTimeFormat() == null ? DEFAULT_TIME_FORMAT : extendedFormatOptions.getTimeFormat();
    } else if (CompleteType.TIMESTAMP.getType().equals(fieldType)) {
      formatString = extendedFormatOptions.getTimeStampFormat() == null ? DEFAULT_TIMESTAMP_FORMAT : extendedFormatOptions.getTimeStampFormat();
    }

    return formatString;
  }

  private static boolean isTimezonePresentInFormat(String formatString) {
    return formatString.toUpperCase().endsWith("TZD");
  }

  private static String replaceTimeZoneWithOffsetFormatString(String formatString) {
    formatString = formatString.substring(0, formatString.length() - 3) + "TZO";
    return formatString;
  }

  private static String replaceTimeZoneWithOffsetInputString(String input) {
    String tzAbbr = "";
    for(int i = input.length() - 1; i >= 0; i--) {
      char c = input.charAt(i);
      if( (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ) {
        tzAbbr =  c + tzAbbr;
      } else {
        break;
      }
    }

    Optional<String> offset = TimezoneAbbreviations.getOffset(tzAbbr);
    if(!offset.isPresent()) {
      throw new RuntimeException(String.format("Invalid timezone abbreviation \"%s\" found in input: \"%s\"", tzAbbr, input));
    }

    input = input.substring(0, input.length() - tzAbbr.length()) + offset.get();
    return input;
  }
}
