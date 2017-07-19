/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.explore;

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;
import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;

import com.dremio.dac.explore.JSONElementLocator.JsonPath;
import com.dremio.dac.explore.JSONElementLocator.JsonPathElement;
import com.dremio.dac.explore.model.extract.MapSelection;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractMapRule;
import com.google.common.base.Joiner;

/**
 * Extract map transformation recommendation suggestions and generating examples and number of matches in sample data
 * for each recommendation.
 */
public class ExtractMapRecommender extends Recommender<ExtractMapRule, MapSelection> {

  @Override
  public List<ExtractMapRule> getRules(MapSelection selection, DataType selColType) {
    checkArgument(selColType == DataType.MAP, "Extract map entries is supported only on MAP type columns");
    return Collections.singletonList(new ExtractMapRule(Joiner.on(".").join(selection.getMapPathList())));
  }

  @Override
  public TransformRuleWrapper<ExtractMapRule> wrapRule(ExtractMapRule rule) {
    return new ExtractMapTransformRuleWrapper(rule);
  }

  private static class ExtractMapTransformRuleWrapper extends TransformRuleWrapper<ExtractMapRule> {
    private final ExtractMapRule rule;
    private final JsonPath path;

    ExtractMapTransformRuleWrapper(ExtractMapRule rule) {
      this.rule = rule;
      this.path = JSONElementLocator.parsePath("value." + rule.getPath());
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      return String.format("%s IS NOT NULL", getFunctionExpr(input));
    }

    @Override
    public boolean canGenerateExamples() {
      return false;
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      throw new UnsupportedOperationException("Example generation is not supported for extract map transform.");
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      StringBuilder sb = new StringBuilder();
      sb.append(expr);
      for (JsonPathElement e : path) {
        if (e.isArray()) {
          sb.append("[").append(e.asArray().getPosition()).append("]");
        } else if (e.isObject()) {
          sb.append(".").append(quoteIdentifier(e.asObject().getField()));
        } else {
          throw new IllegalArgumentException("Unknown JSON path element " + e);
        }
      }
      return sb.toString();
    }

    @Override
    public ExtractMapRule getRule() {
      return rule;
    }

    @Override
    public String describe() {
      return "extract from map " + rule.getPath();
    }
  }
}
