/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.common.expression.fn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dremio.common.types.TypeProtos.MinorType;

public class CastFunctions {

  private static Map<MinorType, String> TYPE2FUNC = new HashMap<>();
  /** The cast functions that need to be replaced (if
   * "dremio.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> CAST_FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY = new HashMap<>();
  static {
    TYPE2FUNC.put(MinorType.UNION, "castUNION");
    TYPE2FUNC.put(MinorType.BIGINT, "castBIGINT");
    TYPE2FUNC.put(MinorType.INT, "castINT");
    TYPE2FUNC.put(MinorType.BIT, "castBIT");
    TYPE2FUNC.put(MinorType.TINYINT, "castTINYINT");
    TYPE2FUNC.put(MinorType.FLOAT4, "castFLOAT4");
    TYPE2FUNC.put(MinorType.FLOAT8, "castFLOAT8");
    TYPE2FUNC.put(MinorType.VARCHAR, "castVARCHAR");
    TYPE2FUNC.put(MinorType.VAR16CHAR, "castVAR16CHAR");
    TYPE2FUNC.put(MinorType.VARBINARY, "castVARBINARY");
    TYPE2FUNC.put(MinorType.DATE, "castDATE");
    TYPE2FUNC.put(MinorType.TIME, "castTIME");
    TYPE2FUNC.put(MinorType.TIMESTAMP, "castTIMESTAMP");
    TYPE2FUNC.put(MinorType.TIMESTAMPTZ, "castTIMESTAMPTZ");
    TYPE2FUNC.put(MinorType.INTERVALDAY, "castINTERVALDAY");
    TYPE2FUNC.put(MinorType.INTERVALYEAR, "castINTERVALYEAR");
    TYPE2FUNC.put(MinorType.INTERVAL, "castINTERVAL");
    TYPE2FUNC.put(MinorType.DECIMAL, "castDECIMAL");

    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.BIGINT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT4));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT8));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL));

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL), "castEmptyStringVarCharToNullableDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVar16CharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVar16CharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVar16CharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVar16CharToNullableFLOAT8");

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVarBinaryToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVarBinaryToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVarBinaryToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVarBinaryToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL), "castEmptyStringVarBinaryToNullableDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL), "castEmptyStringNullableVarCharToNullableDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVar16CharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVar16CharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVar16CharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVar16CharToNullableFLOAT8");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVarBinaryToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVarBinaryToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVarBinaryToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVarBinaryToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL), "castEmptyStringNullableVarBinaryToNullableDECIMAL");
  }

  /**
  * Given the target type, get the appropriate cast function
  * @param targetMinorType the target data type
  * @return the name of cast function
  */
  public static String getCastFunc(MinorType targetMinorType) {
    String func = TYPE2FUNC.get(targetMinorType);
    if (func != null) {
      return func;
    }

    throw new RuntimeException(
      String.format("cast function for type %s is not defined", targetMinorType.name()));
  }

  /**
  * Get a replacing cast function for the original function, based on the specified data mode
  * @param originalCastFunction original cast function
  * @param inputType input (minor) type for cast
  * @return the name of replaced cast function
  */
  public static String getReplacingCastFunction(String originalCastFunction, MinorType inputType) {
    return getReplacingCastFunctionFromNullable(originalCastFunction, inputType);
  }

  /**
  * Check if a replacing cast function is available for the the original function
  * @param originalfunction original cast function
  * @param inputType input (minor) type for cast
  * @return true if replacement is needed, false - if isn't
  */
  public static boolean isReplacementNeeded(String originalfunction, MinorType inputType) {
    return (inputType == MinorType.VARCHAR || inputType == MinorType.VARBINARY || inputType == MinorType.VAR16CHAR) &&
        CAST_FUNC_REPLACEMENT_NEEDED.contains(originalfunction);
  }

  private static String getReplacingCastFunctionFromNullable(String originalCastFunction, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VAR16CHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VARBINARY && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }
}
