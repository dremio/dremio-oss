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
package org.apache.iceberg;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Helper class to construct column name to column ID mapping from Iceberg schema
 */
public class DremioIndexByName extends TypeUtil.CustomOrderSchemaVisitor<Map<String, Integer>> {
  private static final Joiner DOT = Joiner.on(".");

  private final Deque<String> fieldNames = Lists.newLinkedList();
  private final Map<String, Integer> nameToId = Maps.newHashMap();

  @Override
  public Map<String, Integer> schema(Schema schema, Supplier<Map<String, Integer>> structResult) {
    return structResult.get();
  }

  @Override
  public Map<String, Integer> struct(Types.StructType struct, Iterable<Map<String, Integer>> fieldResults) {
    // iterate through the fields to update the index for each one, use size to avoid errorprone failure
    Lists.newArrayList(fieldResults).size();
    return nameToId;
  }

  @Override
  public Map<String, Integer> field(Types.NestedField field, Supplier<Map<String, Integer>> fieldResult) {
    withName(field.name(), fieldResult::get);
    addField(field.name(), field.fieldId());
    return null;
  }

  @Override
  public Map<String, Integer> list(Types.ListType list, Supplier<Map<String, Integer>> elementResult) {
    // add element
    Preconditions.checkState(list.fields().size() == 1);
    addField("list.element", list.fields().get(0).fieldId());

    withName("list.element", elementResult::get);

    return null;
  }

  @Override
  public Map<String, Integer> map(Types.MapType map,
                                  Supplier<Map<String, Integer>> keyResult,
                                  Supplier<Map<String, Integer>> valueResult) {
    withName("key", keyResult::get);

    // add key and value
    for (Types.NestedField field : map.fields()) {
      addField(field.name(), field.fieldId());
    }

    if (map.valueType().isStructType()) {
      // return to avoid errorprone failure
      return valueResult.get();
    }

    withName("value", valueResult::get);

    return null;
  }

  private <T> T withName(String name, Callable<T> callable) {
    fieldNames.push(name);
    try {
      return callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      fieldNames.pop();
    }
  }

  private void addField(String name, int fieldId) {
    String fullName = name;
    if (!fieldNames.isEmpty()) {
      fullName = DOT.join(DOT.join(fieldNames.descendingIterator()), name);
    }

    Integer existingFieldId = nameToId.put(fullName, fieldId);
    if (existingFieldId != null && !"list.element".equals(name) && !"value".equals(name)) {
      throw new ValidationException(
        "Invalid schema: multiple fields for name %s: %s and %s", fullName, existingFieldId, fieldId);
    }
  }
}
