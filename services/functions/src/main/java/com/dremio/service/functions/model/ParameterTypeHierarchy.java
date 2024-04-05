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

package com.dremio.service.functions.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;

public class ParameterTypeHierarchy {
  private static final ImmutableMap<ParameterType, ImmutableSet<ParameterType>> tree =
      new ImmutableMap.Builder<ParameterType, ImmutableSet<ParameterType>>()
          .put(
              ParameterType.ANY,
              ImmutableSet.of(ParameterType.PRIMITIVE, ParameterType.SEMISTRUCTURED))
          .put(
              ParameterType.PRIMITIVE,
              ImmutableSet.of(
                  ParameterType.BOOLEAN,
                  ParameterType.NUMERIC,
                  ParameterType.STRING,
                  ParameterType.DATEANDTIME))
          .put(ParameterType.STRING, ImmutableSet.of(ParameterType.BYTES, ParameterType.CHARACTERS))
          .put(
              ParameterType.NUMERIC,
              ImmutableSet.of(
                  ParameterType.FLOAT,
                  ParameterType.DECIMAL,
                  ParameterType.DOUBLE,
                  ParameterType.INT,
                  ParameterType.BIGINT))
          .put(
              ParameterType.DATEANDTIME,
              ImmutableSet.of(ParameterType.DATE, ParameterType.TIME, ParameterType.TIMESTAMP /*,
        ParameterType.INTERVAL*/))
          .put(
              ParameterType.SEMISTRUCTURED, ImmutableSet.of(ParameterType.ARRAY, ParameterType.MAP))
          .build();

  public static Optional<ImmutableSet<ParameterType>> getChildren(ParameterType parameterType) {
    return Optional.ofNullable(tree.get(parameterType));
  }

  public static Optional<ParameterType> getParent(ParameterType parameterType) {
    for (ParameterType parent : tree.keySet()) {
      ImmutableSet<ParameterType> children = tree.get(parent);
      if (children.contains(parameterType)) {
        return Optional.of(parent);
      }
    }

    return Optional.empty();
  }

  public static boolean isDescendantOf(ParameterType a, ParameterType b) {
    if (a == b) {
      return true;
    }

    Optional<ImmutableSet<ParameterType>> optionalChildren = ParameterTypeHierarchy.getChildren(a);
    if (!optionalChildren.isPresent()) {
      return false;
    }

    return optionalChildren.get().stream().anyMatch(child -> isDescendantOf(child, b));
  }
}
