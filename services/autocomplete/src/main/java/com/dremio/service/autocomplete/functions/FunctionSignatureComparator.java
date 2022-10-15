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

import com.google.common.collect.ImmutableList;

public final class FunctionSignatureComparator implements Comparator<FunctionSignature> {
  public static final FunctionSignatureComparator INSTANCE = new FunctionSignatureComparator();

  private FunctionSignatureComparator() {}

  @Override
  public int compare(FunctionSignature first, FunctionSignature second) {
    int cmp;

    cmp = compare(first.getParameters(), second.getParameters());
    if (cmp != 0) {
      return cmp;
    }

    return first.getReturnType().compareTo(second.getReturnType());
  }

  private static int compare(ImmutableList<Parameter> parameters1, ImmutableList<Parameter> parameters2) {
    int cmp = Integer.compare(parameters1.size(), parameters2.size());
    if (cmp != 0) {
      return cmp;
    }

    for (int i = 0; i < parameters1.size(); i++) {
      Parameter parameter1 = parameters1.get(i);
      Parameter parameter2 = parameters2.get(i);
      cmp = parameter1.getType().compareTo(parameter2.getType());
      if (cmp != 0) {
        return cmp;
      }

      cmp = parameter1.getKind().compareTo(parameter2.getKind());
      if (cmp != 0) {
        return cmp;
      }
    }

    return cmp;
  }
}
