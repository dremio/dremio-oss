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
package com.dremio;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;

/**
 * Utilities useful to construct Arrow data structures
 */
public class ArrowDsUtil {
  public static JsonStringArrayList<Integer> intList(Integer... ints) {
    JsonStringArrayList<Integer> list = new JsonStringArrayList<>(ints.length);
    list.addAll(Arrays.asList(ints));
    return list;
  }

  public static JsonStringArrayList<Long> longList(Long... longs) {
    JsonStringArrayList<Long> list = new JsonStringArrayList<>(longs.length);
    list.addAll(Arrays.asList(longs));
    return list;
  }

  public static JsonStringArrayList<Float> floatList(Float aFloat) {
    JsonStringArrayList<Float> list = new JsonStringArrayList<>(1);
    list.add(aFloat);
    return list;
  }

  public static JsonStringArrayList<Double> doubleList(Double aDouble) {
    JsonStringArrayList<Double> list = new JsonStringArrayList<>(1);
    list.add(aDouble);
    return list;
  }

  public static JsonStringArrayList<Double> doubleList(Double... doubles) {
    JsonStringArrayList<Double> list = new JsonStringArrayList<>(doubles.length);
    list.addAll(Arrays.asList(doubles));
    return list;
  }

  public static JsonStringArrayList<BigDecimal> decimalList(String value) {
    JsonStringArrayList<BigDecimal> list = new JsonStringArrayList<>(1);
    list.add(new BigDecimal(value));
    return list;
  }

  public static JsonStringArrayList<BigDecimal> listOfDecimals(List<String> values) {
    JsonStringArrayList<BigDecimal> list = new JsonStringArrayList<>(values.size());
    for (String value : values) {
      list.add(new BigDecimal(value));
    }
    return list;
  }

  public static JsonStringArrayList<Text> textList(String text) {
    JsonStringArrayList<Text> list = new JsonStringArrayList<>(1);
    list.add(new Text(text));
    return list;
  }

  public static JsonStringArrayList<Text> textList(String... texts) {
    JsonStringArrayList<Text> list = new JsonStringArrayList<>(texts.length);
    for (String text : texts) {
      list.add(new Text(text));
    }
    return list;
  }

  public static JsonStringHashMap<String, Object> longStruct(String fieldName, Long... longs) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    for (Long aLong : longs) {
      structrow.put(fieldName, aLong);
    }
    return structrow;
  }

  public static JsonStringHashMap<String, Object> nullStruct(String fieldName) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    structrow.put(fieldName, null);
    return structrow;
  }

  public static JsonStringHashMap<String, Object> intStruct(String fieldName, Integer... integers) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    for (Integer aInt : integers) {
      structrow.put(fieldName, aInt);
    }
    return structrow;
  }


  public static JsonStringHashMap<String, Object> doubleStruct(String fieldName, Double... doubles) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    for (Double aDouble : doubles) {
      structrow.put(fieldName, aDouble);
    }
    return structrow;
  }

  public static JsonStringHashMap<String, Object> textStruct(String fieldName, String... texts) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    for (String text : texts) {
      org.apache.arrow.vector.util.Text value = new Text(text);
      structrow.put(fieldName, value);
    }
    return structrow;
  }

  public static JsonStringHashMap<String, Object> decimalStruct(String fieldName, String... texts) {
    JsonStringHashMap<String, Object> structrow = new JsonStringHashMap<>();
    for (String text : texts) {
      structrow.put(fieldName, new BigDecimal(text));
    }
    return structrow;
  }

  public static JsonStringArrayList<JsonStringArrayList<Long>> wrapListInList(JsonStringArrayList<Long> underlying) {
    JsonStringArrayList<JsonStringArrayList<Long>> list = new JsonStringArrayList<>();
    list.add(underlying);
    return list;
  }

  public static JsonStringArrayList<JsonStringArrayList<Double>> wrapDoubleListInList(JsonStringArrayList<Double> underlying) {
    JsonStringArrayList<JsonStringArrayList<Double>> list = new JsonStringArrayList<>();
    list.add(underlying);
    return list;
  }

  public static JsonStringArrayList<JsonStringArrayList<Text>> wrapTextListInList(JsonStringArrayList<Text> underlying) {
    JsonStringArrayList<JsonStringArrayList<Text>> list = new JsonStringArrayList<>();
    list.add(underlying);
    return list;
  }

  public static JsonStringHashMap<String, JsonStringHashMap<String, Object>> wrapStructInStruct(String fieldName, JsonStringHashMap<String, Object> struct) {
    JsonStringHashMap<String, JsonStringHashMap<String, Object>> structrow = new JsonStringHashMap<>();
    structrow.put(fieldName, struct);
    return structrow;
  }

  public static JsonStringArrayList<JsonStringHashMap<String, Object>> wrapStructInList(JsonStringHashMap<String, Object>... structs) {
    JsonStringArrayList<JsonStringHashMap<String, Object>> list = new JsonStringArrayList<>();
    list.addAll(Arrays.asList(structs));
    return list;
  }

  public static JsonStringHashMap<String, JsonStringArrayList<?>> wrapListInStruct(String fieldName, JsonStringArrayList<?> underlying) {
    JsonStringHashMap<String, JsonStringArrayList<?>> structrow = new JsonStringHashMap<>();
    structrow.put(fieldName, underlying);
    return structrow;
  }
}
