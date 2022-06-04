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

import static com.dremio.reflection.hints.ExplanationUtil.disjointFilterExplanation;
import static com.dremio.reflection.hints.ExplanationUtil.fieldMissingExplanation;
import static com.dremio.reflection.hints.ExplanationUtil.filterOverSpecified;
import static com.google.common.collect.ImmutableList.toImmutableList;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.math.exception.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.reflection.hints.features.FieldMissingFeature;
import com.dremio.reflection.hints.features.FilterDisjointFeature;
import com.dremio.reflection.hints.features.HintFeature;
import com.dremio.reflection.hints.features.MaterializationFilterOverSpecifiedFeature;
import com.dremio.sabot.kernel.proto.ReflectionExplanation;

public class ReflectionExplanationManager {
  public static final int MAX_REFLECTIONS_TO_DISPLAY_TO_SHOW = 5;
  public static final int MAX_NUMBER_OF_HINTS_PER_REFLECTION = 5;
  private final static Logger LOGGER = LoggerFactory.getLogger(ReflectionExplanationManager.class);
  private final ReflectionExplanationFeatureGatherer reflectionExplanationFeatureGatherer;

  public ReflectionExplanationManager(ReflectionExplanationFeatureGatherer reflectionExplanationFeatureGatherer) {
    this.reflectionExplanationFeatureGatherer = reflectionExplanationFeatureGatherer;
  }

  public Stream<ReflectionExplanationsAndQueryDistance> generateDisplayExplanations() {
    List<ReflectionExplanationsAndQueryDistance> reflectionExplanationsAndQueryDistanceList
      = reflectionExplanationFeatureGatherer.reflectionIdToFeatureList.entrySet()
      .stream()
      .filter(e -> e.getValue().size() <= MAX_NUMBER_OF_HINTS_PER_REFLECTION)
      .map(this::entryToDisplayHint)
      .filter(this::nonZeroDistance)
      .sorted()
      .collect(Collectors.toList());

    for (int i = 0; i < reflectionExplanationsAndQueryDistanceList.size(); i++) {
      ReflectionExplanationsAndQueryDistance r = reflectionExplanationsAndQueryDistanceList.get(i);
      if (i < MAX_REFLECTIONS_TO_DISPLAY_TO_SHOW) {
        decorateWithDisplayMessages(r);
      } else {
        r.hintHidden = true;
      }
    }
    return reflectionExplanationsAndQueryDistanceList.stream();
  }

  private ReflectionExplanationsAndQueryDistance entryToDisplayHint(
    Entry<String, Set<HintFeature>> reflectionIdAndFeatureSet) {
    return new ReflectionExplanationsAndQueryDistance(reflectionIdAndFeatureSet.getKey(),
      queryDistance(reflectionIdAndFeatureSet.getValue()));
  }

  private void decorateWithDisplayMessages(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    String reflectionId = reflectionExplanationsAndQueryDistance.reflectionId;
    Set<HintFeature> hintFeatureSet = reflectionExplanationFeatureGatherer.reflectionIdToFeatureList.get(reflectionId);
    reflectionExplanationsAndQueryDistance.displayHintMessageList = hintFeatureSet
      .stream()
      .map(this::featureToExplanation)
      .collect(toImmutableList());
  }

  private ReflectionExplanation featureToExplanation(HintFeature hintFeature) {
    if(hintFeature instanceof FieldMissingFeature){
      return fieldMissingToDisplayHintMessage((FieldMissingFeature) hintFeature);
    } else if(hintFeature instanceof MaterializationFilterOverSpecifiedFeature) {
      return featureToExplanation((MaterializationFilterOverSpecifiedFeature)hintFeature);
    } else if(hintFeature instanceof FilterDisjointFeature) {
      return clauseToDisplayHintMessage((FilterDisjointFeature) hintFeature);
    } else {
      throw new RuntimeException("Unknown Type" + hintFeature.getClass());
    }
  }

  private ReflectionExplanation clauseToDisplayHintMessage(FilterDisjointFeature filterDisjointFeature) {
    return disjointFilterExplanation(getFilterWithColumnName(filterDisjointFeature.getMaterializationFilter(), filterDisjointFeature.getDatasetRowType()));
  }

  private ReflectionExplanation fieldMissingToDisplayHintMessage(FieldMissingFeature fieldMissingFeature){
    try {
      List<String> names = fieldMissingFeature.getUserQueryNode().getRowType().getFieldNames();
      if(names.size() > fieldMissingFeature.getIndex()) {
        return fieldMissingExplanation(names.get(fieldMissingFeature.getIndex()), fieldMissingFeature.getIndex());
      } else {
        return fieldMissingExplanation(fieldMissingFeature.getName(), fieldMissingFeature.getIndex());
      }
    } catch (Exception ex) {
      LOGGER.warn("Failed to create display data",  ex);
      return fieldMissingExplanation(
          fieldMissingFeature.getName(),
          fieldMissingFeature.getIndex());
    }
  }

  private ReflectionExplanation featureToExplanation(
      MaterializationFilterOverSpecifiedFeature materializationFilterOverSpecified) {
    return filterOverSpecified(getFilterWithColumnName(materializationFilterOverSpecified.getMaterializationFilter(), materializationFilterOverSpecified.getDatasetRowType()));
  }

  private boolean nonZeroDistance(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    return reflectionExplanationsAndQueryDistance.queryDistance != 0.0;
  }

  private double queryDistance(Set<HintFeature> hintFeatureSet) {
    return hintFeatureSet.size();
  }

  // Get filter and rewrite rex index with column name
  private String getFilterWithColumnName(RexNode materializationFilter, RelDataType datasetRowType) throws NullArgumentException {
    StringBuilder modified = new StringBuilder();

    String original = materializationFilter.toString();
    List<String> fields = datasetRowType.getFieldNames();

    if (fields == null) {
      throw new NullArgumentException();
    }

    int index = 0;
    while (index < original.length()) {
      if (original.charAt(index) == '\'') {
        // Index after the closing single quote
        int nextIndex = original.indexOf('\'', index + 1);
        nextIndex =  nextIndex != -1 ? nextIndex + 1 : index + 1;
        modified.append(original, index, nextIndex);
        index = nextIndex;
        continue;
      }
      if (original.charAt(index) == '$') {
        // Index after numbers after $
        int nextIndex = index + 1;
        while (nextIndex < original.length() && Character.isDigit(original.charAt(nextIndex))) {
          nextIndex++;
        }
        int columnIndex = Integer.valueOf(original.substring(index + 1, nextIndex));
        modified.append((nextIndex - index == 1 || columnIndex >= fields.size()) ? '$' : fields.get(columnIndex));
        index = nextIndex;
        continue;
      }
      modified.append(original.charAt(index));
      index++;
    }
    return modified.toString();
  }
}
