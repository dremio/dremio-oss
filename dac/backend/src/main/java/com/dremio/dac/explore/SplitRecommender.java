/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.common.utils.SqlUtils.stringLiteral;
import static com.dremio.dac.proto.model.dataset.MatchType.exact;
import static com.dremio.dac.proto.model.dataset.MatchType.regex;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.regex.Pattern.quote;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.explore.udfs.SplitPattern;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.SplitPositionType;
import com.dremio.dac.proto.model.dataset.SplitRule;

/**
 * Split transformation recommendation suggestions and generating examples and number of matches in sample data
 * for each recommendation.
 */
public class SplitRecommender extends Recommender<SplitRule, Selection> {
  private static final Logger logger = LoggerFactory.getLogger(SplitRecommender.class);

  @Override
  public List<SplitRule> getRules(Selection selection, DataType selColType) {
    checkArgument(selColType == DataType.TEXT, "Split is supported only on TEXT type columns");

    List<SplitRule> rules = new ArrayList<>();
    String seltext = selection.getCellText().substring(selection.getOffset(), selection.getOffset() + selection.getLength());
    rules.add(new SplitRule(seltext, exact, false));
    if (!seltext.toUpperCase().equals(seltext.toLowerCase())) {
      rules.add(new SplitRule(seltext, exact, true));
    }
    return rules;
  }

  public TransformRuleWrapper<SplitRule> wrapRule(SplitRule rule) {
    return new SplitTransformRuleWrapper(rule);
  }

  private static class SplitTransformRuleWrapper extends TransformRuleWrapper<SplitRule> {
    private final SplitRule rule;

    SplitTransformRuleWrapper(SplitRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      return String.format("%s(%s, %s)",
          "regexp_matches",
          input,
          getDelimiterRegexLiteral(true)
      );
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      return String.format("%s(%s, %s)",
          SplitPattern.REGEXP_SPLIT_POSITIONS,
          input,
          getDelimiterRegexLiteral(false)
      );
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      checkArgument(args.length == 2 && args[0] != null && args[1] != null, "Expected the split position type and index as arguments");
      SplitPositionType splitPositionType = (SplitPositionType) args[0];
      Integer index = (Integer) args[1];

      return String.format("%s(%s, %s, %s, %d)",
          SplitPattern.REGEXP_SPLIT,
          expr,
          getDelimiterRegexLiteral(false),
          stringLiteral(splitPositionType.toString()),
          index
      );
    }

    @Override
    public SplitRule getRule() {
      return rule;
    }

    @Override
    public String describe() {
      String suffix = (rule.getIgnoreCase() != null && rule.getIgnoreCase()) ? " ignore case" : "";
      if (rule.getMatchType() == exact) {
        return String.format("Exactly matches \"%s\"", rule.getPattern()) + suffix;
      } else if (rule.getMatchType() == regex) {
        return String.format("Matches regex \"%s\"", rule.getPattern()) + suffix;
      }
      throw UserException.unsupportedError()
          .message("unsupported selection type: " + rule.getMatchType().toString())
          .build(logger);
    }

    private String getDelimiterRegexLiteral(boolean forMatching) {
      String regexp;
      switch (rule.getMatchType()) {
        case exact:
          regexp = quote(rule.getPattern());
          break;
        case regex:
          regexp = rule.getPattern();
          break;
        default:
          throw UserException.unsupportedError()
              .message("unsupported selection type: " + rule.getMatchType().toString())
              .build(logger);
      }

      if (forMatching) {
        regexp = ".*" + regexp + ".*";
      }

      if (rule.getIgnoreCase() != null && rule.getIgnoreCase()) {
        // make the pattern case insensitive and unicode case aware
        regexp = "(?i)(?u)" + regexp;
      }

      return stringLiteral(regexp);
    }
  }
}
