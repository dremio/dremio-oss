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
package com.dremio.exec.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * Rewrite inputs in expression to handle any RowType change during expansion
 */
public class ExpressionInputRewriter extends RexShuttle {
  private final RexBuilder builder;
  private final List<String> fieldNames;
  private final RelNode input;
  private final Map<Integer, Integer> fieldMap;
  private final String suffix;

  public ExpressionInputRewriter(RexBuilder builder, RelDataType rowType, RelNode input, String suffix) {
    this.builder = builder;
    this.input = input;
    this.fieldNames = rowType.getFieldNames();
    this.fieldMap = new HashMap<>();
    this.suffix = suffix;
    final List<String> inputFields = input.getRowType().getFieldNames();
    for(int i = 0 ; i < fieldNames.size() ; i++) {
      String fieldName = fieldNames.get(i);
      fieldMap.put(i, inputFields.indexOf(fieldName + suffix));
    }
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    int originalIndex = inputRef.getIndex();
    int newIndex = fieldMap.get(inputRef.getIndex());
    if (newIndex == -1) {
      throw new IllegalArgumentException(String.format("Could not find field, %s, from, %s", fieldNames.get(originalIndex) + suffix, input.getRowType()));
    }
    return builder.makeInputRef(input, newIndex);
  }
}
