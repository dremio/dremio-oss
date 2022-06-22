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
package com.dremio.service.autocomplete.functions;

import java.util.Comparator;

import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

public final class FunctionSignatureComparator implements Comparator<FunctionSignature> {
  public static final FunctionSignatureComparator INSTANCE = new FunctionSignatureComparator();

  private FunctionSignatureComparator() {}

  @Override
  public int compare(FunctionSignature first, FunctionSignature second) {
    int cmp;

    cmp = compare(first.getTemplate(), second.getTemplate());
    if (cmp != 0) {
      return cmp;
    }

    cmp = compare(first.getOperandTypes(), second.getOperandTypes());
    if (cmp != 0) {
      return cmp;
    }

    return first.getReturnType().compareTo(second.getReturnType());
  }

  private static int compare(ImmutableList<SqlTypeName> operands1, ImmutableList<SqlTypeName> operands2) {
    int cmp = Integer.compare(operands1.size(), operands2.size());
    if (cmp != 0) {
      return cmp;
    }

    for (int i = 0; i < operands1.size(); i++) {
      SqlTypeName firstType = operands1.get(i);
      SqlTypeName secondType = operands2.get(i);
      cmp = firstType.compareTo(secondType);
      if (cmp != 0) {
        return cmp;
      }
    }

    return cmp;
  }

  private static int compare(String firstTemplate, String secondTemplate) {
    boolean firstTemplateNull = firstTemplate == null;
    boolean secondTemplateNull = secondTemplate == null;

    if (!firstTemplateNull && secondTemplateNull) {
      return -1;
    } else if (firstTemplateNull && !secondTemplateNull) {
      return 1;
    } else if (firstTemplateNull && secondTemplateNull) {
      return 0;
    } else {
      return firstTemplate.compareTo(secondTemplate);
    }
  }
}
