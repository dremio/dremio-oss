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
package com.dremio.service.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;

/**
 * Class to determine if a function should be excluded from the docs.
 */
public final class ExcludedFunctions {

  private ExcludedFunctions() {}

  public static boolean shouldExcludeFunction(String name) {
    return reasonForExcludingFunction(name) != null;
  }

  public static String reasonForExcludingFunction(String name) {
    if (isKeyword(name)) {
      return "'" + name + "'" + " is a keyword and should not appear as a function.";
    }

    if (isOperator(name)) {
      return "'" + name + "'" + " is an operator and should not appear as a function.";
    }

    String variantName = matchVariant(name);
    if (variantName != null) {
      return name + " is an variant of " + "'" + variantName + "'" + "and should not appear as a function.";
    }

    if (isInternalFunction(name)) {
      return "'" + name + "'" + " is an internal function and should not appear as a function.";
    }

    if (isJsonFunction(name)) {
      return "'" + name + "'" + " is an unimplemented JSON function and should not appear as a function.";
    }

    return null;
  }

  private static boolean isKeyword(String name) {
    return name.contains(" ");
  }

  private static boolean isOperator(String name) {
    // We want to ignore functions like "<=" since they are more operators rather than functions
    if (allNonAlphanumeric(name)) {
      return true;
    }

    ImmutableSet<String> names = ImmutableSet.of(
      "ADD",
      "AND",
      "BOOLEANAND",
      "BOOLEANOR",
      "DIV",
      "DIVIDE",
      "EQ",
      "EQUAL",
      "GREATER_THAN",
      "GREATER_THAN_OR_EQUAL_TO",
      "LESS_THAN",
      "LESS_THAN_OR_EQUAL_TO",
      "MOD",
      "MODULO",
      "MULTIPLY",
      "NEGATIVE",
      "NOT",
      "NOT_EQUAL",
      "OR",
      "ORNOSHORTCIRCUIT",
      "POSITIVE",
      "SUBTRACT"
    );

    return names.contains(name);
  }

  private static boolean allNonAlphanumeric(String name) {
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLetterOrDigit(name.charAt(i))) {
        return false;
      }
    }

    return true;
  }

  private static String matchVariant(String name) {
    Map<String, Function<String, Boolean>> variantDetectors = getVariantDetectors();
    for (String variantName : variantDetectors.keySet()) {
      if (variantDetectors.get(variantName).apply(name)) {
        return variantName;
      }
    }

    return null;
  }

  private static Map<String, Function<String, Boolean>> getVariantDetectors() {
    Map<String, Function<String, Boolean>> detectors = new HashMap<>();
    detectors.put("$", name -> sharesPrefix(name, "$"));
    detectors.put("_", name -> sharesPrefix(name, "_"));
    detectors.put("ADD", name -> sharesPrefix(name, "ADD"));
    detectors.put("ALTERNATE", name -> name.equals("ALTERNATE3"));
    detectors.put("ARRAY_MAX", name -> sharesPrefix(name, "ARRAY_MAX"));
    detectors.put("ARRAY_MIN", name -> sharesPrefix(name, "ARRAY_MIN"));
    detectors.put("ARRAY_SUM", name -> sharesPrefix(name, "ARRAY_SUM"));
    detectors.put("ASSERT", name -> sharesPrefix(name, "ASSERT"));
    detectors.put("CAST", name -> sharesPrefix(name, "CAST"));
    detectors.put("CEILING", name -> name.equals("CEIL"));
    detectors.put("COMPARE_TO", name -> name.startsWith("COMPARE_TO"));
    detectors.put("CONCAT", name -> name.equals("CONCATOPERATOR"));
    detectors.put("CONVERT_FROM", name -> sharesPrefix(name, "CONVERT_FROM"));
    detectors.put("CONVERT_TO", name -> sharesPrefix(name, "CONVERT_TO"));
    detectors.put("CORR", name -> name.equals("CORRELATION"));
    detectors.put("COVAR_SAMP", name -> name.equals("COVARIANCE"));
    detectors.put("CURRENT_TIMESTAMP", name -> name.equals("CURRENT_TIMESTAMP_UTC"));
    detectors.put("CURRENT_TIME", name -> name.equals("CURRENT_TIME_UTC"));
    detectors.put("DATE_TRUNC", name -> sharesPrefix(name, "DATE_TRUNC"));
    detectors.put("EXTRACT", name -> sharesPrefix(name, "EXTRACT"));
    detectors.put("HASH", name -> !"HASH64".equals(name) && sharesPrefix(name, "HASH"));
    detectors.put("HLL", name -> sharesPrefix(name, "HLL"));
    detectors.put("INTERVAL", name -> sharesPrefix(name, "INTERVAL_"));
    detectors.put("ITEMS_SKETCH", name -> sharesPrefix(name, "ITEMS_SKETCH_"));
    detectors.put("LENGTH", name -> name.equals("LENGTHUTF8"));
    detectors.put("MAX", name -> name.equals("MAX_V2"));
    detectors.put("MIN", name -> name.equals("MIN_V2"));
    detectors.put("RANDOM", name -> ImmutableSet.of("RAND", "RANDOMBIGINT", "RANDOMFLOAT8").contains(name));
    detectors.put("SIMILAR_TO", name -> name.equals("SIMILAR"));
    detectors.put("SUBSTRING", name -> ImmutableSet.of("BYTESUBSTRING", "BYTE_SUBSTR", "CHARSUBSTRING", "SUBSTR", "SUBSTR2", "SUBSTRING2").contains(name));
    detectors.put("SUM", name -> name.equals("SUM_V2"));
    detectors.put("TIMESTAMPADD", name -> sharesPrefix(name, "TIMESTAMPADD"));
    detectors.put("TIMESTAMPDIFF", name -> sharesPrefix(name, "TIMESTAMPDIFF"));
    detectors.put("TRUNCATE", name -> name.equals("TRUNC"));
    detectors.put("VAR_SAMP", name -> name.equals("VARIANCE"));
    return detectors;
  }

  private static boolean sharesPrefix(String name, String prefix) {
    return name.startsWith(prefix) && !name.equals(prefix);
  }

  private static boolean isInternalFunction(String name) {
    return ImmutableSet.of(
      "COMPARETYPE", "DREMIOSPLITDISTRIBUTE", "EVERY",
      "ICEBERGDISTRIBUTEBYPARTITION", "INCREASINGBIGINT", "ITEMS_SKETCH",
      "KVGEN", "LEAKRESOURCE", "NEWPARTITIONNUMBER", "NEWPARTITIONVALUE", "NONNULLSTATCOUNT",
      "PARTITIONBITCOUNTER", "PIVOT", "SINGLE_VALUE", "STATCOUNT", "STATEMENT_TIMESTAMP",
      "TDIGEST", "TDIGEST_MERGE", "TDIGEST_QUANTILE", "TIMEOFDAY", "U-", "UNPIVOT", "LAST_MATCHING_MAP_ENTRY_FOR_KEY",
      "LOCAL_LISTAGG", "LISTAGG_MERGE", "PHASE1_ARRAY_AGG", "PHASE2_ARRAY_AGG", "ARRAY_SORT", "LIST_TO_DELIMITED_STRING",
      "IDENTITY", "NULLABLE")
      .contains(name);
  }

  private static boolean isJsonFunction(String name) {
    return ImmutableSet.of(
        "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_ARRAYAGG_ABSENT_ON_NULL",
        "JSON_ARRAYAGG_NULL_ON_NULL", "JSON_EXISTS", "JSON_OBJECT",
        "JSON_OBJECTAGG", "JSON_OBJECTAGG_ABSENT_ON_NULL",
        "JSON_OBJECTAGG_NULL_ON_NULL", "JSON_QUERY",
        "JSON_VALUE", "JSON_VALUE_ANY")
      .contains(name);
  }
}
