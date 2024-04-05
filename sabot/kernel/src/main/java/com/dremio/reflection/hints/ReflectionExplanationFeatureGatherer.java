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
package com.dremio.reflection.hints;

import com.dremio.reflection.hints.features.FieldMissingFeature;
import com.dremio.reflection.hints.features.FilterDisjointFeature;
import com.dremio.reflection.hints.features.HintFeature;
import com.dremio.reflection.hints.features.MaterializationFilterOverSpecifiedFeature;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * A gather of hints
 *
 * <p>TODO better comment
 */
public class ReflectionExplanationFeatureGatherer {
  private static final Function<String, Set<HintFeature>> KEY_TO_NEW_SET = (key) -> new HashSet<>();

  public Map<String, Set<HintFeature>> reflectionIdToFeatureList = new HashMap<>();
  public String currentReflectionId;

  public void reportFieldMissing(RelNode userQueryNode, int index, String name) {
    reflectionIdToFeatureList
        .computeIfAbsent(currentReflectionId, KEY_TO_NEW_SET)
        .add(new FieldMissingFeature(userQueryNode, index, name));
  }

  /**
   * Clause is overly specific
   *
   * @param userFilter
   * @param materializationFilter
   */
  public void overFiltered(
      RexNode userFilter, RexNode materializationFilter, RelDataType datasetRowType) {
    reflectionIdToFeatureList
        .computeIfAbsent(currentReflectionId, KEY_TO_NEW_SET)
        .add(new FilterDisjointFeature(userFilter, materializationFilter, datasetRowType));
  }

  /**
   * Clause over filters
   *
   * @param materializationFilters
   */
  public void overFiltered(RexNode materializationFilters, RelDataType datasetRowType) {
    reflectionIdToFeatureList
        .computeIfAbsent(currentReflectionId, KEY_TO_NEW_SET)
        .add(new MaterializationFilterOverSpecifiedFeature(materializationFilters, datasetRowType));
  }
}
