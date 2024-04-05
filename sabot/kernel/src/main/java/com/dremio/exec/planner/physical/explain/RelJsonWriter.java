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
package com.dremio.exec.planner.physical.explain;

import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer.OpId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

/**
 * Copied from {@code org.apache.calcite.rel.externalize.RelJsonWriter}. Modified to include
 * Dremio's operator id (major fragment, operator number within fragment)
 *
 * <p>Callback for a relational expression to dump itself as JSON.
 */
public class RelJsonWriter implements RelWriter {
  // ~ Instance fields ----------------------------------------------------------

  private final JsonBuilder jsonBuilder;
  private final RelJson relJson;
  private final Map<Prel, OpId> ids;
  private final Map<String, Object> relExplainMap;
  private final SqlExplainLevel detailLevel;

  private final List<Pair<String, Object>> values = new ArrayList<>();

  // ~ Constructors -------------------------------------------------------------

  public RelJsonWriter(Map<Prel, OpId> ids, SqlExplainLevel detailLevel) {
    this.ids = ids;
    this.detailLevel = detailLevel;
    jsonBuilder = new JsonBuilder();
    relJson = new RelJson(jsonBuilder);
    relExplainMap = Maps.newLinkedHashMap(); // need the ordering
  }

  // ~ Methods ------------------------------------------------------------------

  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    final Map<String, Object> map = Maps.newLinkedHashMap(); // need the ordering
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    putIntoMap(map, "op", relJson.classToTypeName(rel.getClass()));

    final Map<String, Object> valuesMap = jsonBuilder.map();
    for (Pair<String, Object> value : values) {
      if (value.right instanceof RelNode) {
        continue;
      }

      putIntoMap(valuesMap, value.left, escapeSpecialChars(String.valueOf(value.right)));
    }
    putIntoMap(map, "values", valuesMap);

    final List<String> inputOpIds = getInputOpIds(rel.getInputs());
    putIntoMap(map, "inputs", inputOpIds);

    putIntoMap(map, "rowType", escapeSpecialChars(String.valueOf(rel.getRowType())));
    putIntoMap(map, "rowCount", mq.getRowCount(rel));
    putIntoMap(
        map, "cumulativeCost", escapeSpecialChars(String.valueOf(mq.getCumulativeCost(rel))));

    final OpId opId = ids.get(rel);
    putIntoMap(
        relExplainMap, String.format("%02d-%02d", opId.getFragmentId(), opId.getOpId()), map);

    explainInputs(rel.getInputs());
  }

  /** helper method to quote the key and put the entry into map */
  private static void putIntoMap(Map<String, Object> map, String key, Object value) {
    map.put("\"" + key + "\"", value);
  }

  /** Escape the backslash for string output */
  private static String escapeSpecialChars(final String input) {
    return StringUtils.replace(input, "\\", "\\\\");
  }

  private List<String> getInputOpIds(List<RelNode> inputs) {
    final List<String> inputOpIds = Lists.newArrayList();
    for (RelNode input : inputs) {
      final OpId opId = ids.get(input);
      inputOpIds.add(String.format("%02d-%02d", opId.getFragmentId(), opId.getOpId()));
    }
    return inputOpIds;
  }

  private void explainInputs(List<RelNode> inputs) {
    for (RelNode input : inputs) {
      input.explain(this);
    }
  }

  @Override
  public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
    explain_(rel, valueList);
  }

  @Override
  public SqlExplainLevel getDetailLevel() {
    return detailLevel;
  }

  @Override
  public RelWriter input(String term, RelNode input) {
    return this;
  }

  @Override
  public RelWriter item(String term, Object value) {
    values.add(Pair.of(term, value));
    return this;
  }

  @Override
  public RelWriter itemIf(String term, Object value, boolean condition) {
    if (condition) {
      item(term, value);
    }
    return this;
  }

  @Override
  public RelWriter done(RelNode node) {
    final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf(values);
    values.clear();
    explain_(node, valuesCopy);
    return this;
  }

  @Override
  public boolean nest() {
    return true;
  }

  /** Returns a JSON string describing the relational expressions that were just explained. */
  public String asString() {
    return jsonBuilder.toJsonString(relExplainMap);
  }
}
