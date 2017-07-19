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

import static com.dremio.common.utils.SqlUtils.stringLiteral;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_END;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_START;
import static com.dremio.dac.proto.model.dataset.IndexType.INDEX;
import static com.dremio.dac.proto.model.dataset.IndexType.INDEX_BACKWARDS;
import static com.dremio.dac.util.DatasetsUtil.position;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.PatternMatchUtils.CharType;
import com.dremio.dac.explore.PatternMatchUtils.Match;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.explore.udfs.ExtractPattern;
import com.dremio.dac.explore.udfs.ExtractPosition;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Direction;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.ExtractRulePattern;
import com.dremio.dac.proto.model.dataset.ExtractRulePosition;
import com.dremio.dac.proto.model.dataset.Offset;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.dac.util.DatasetsUtil.ExtractRuleVisitor;

/**
 * Extract transformation recommendation suggestions and generating examples and number of matches in sample data
 * for each recommendation.
 */
public class ExtractRecommender extends Recommender<ExtractRule, Selection> {
  private static final Logger logger = LoggerFactory.getLogger(ExtractRecommender.class);

  @Override
  public List<ExtractRule> getRules(Selection selection, DataType selColType) {
    checkArgument(selColType == DataType.TEXT, "Extract text is supported only on TEXT type columns");

    if (selection.getLength() <= 0) {
      throw UserException.validationError()
          .message("text recommendation requires non-empty text selection")
          .build(logger);
    }

    List<ExtractRule> rules = new ArrayList<>();
    rules.addAll(recommendCharacterGroup(selection));
    rules.addAll(recommendPosition(selection));

    return rules;
  }

  public TransformRuleWrapper<ExtractRule> wrapRule(ExtractRule extractRule) {
    switch (extractRule.getType()) {
      case pattern:
        return new ExtractPatternTransformRuleWrapper(extractRule);
      case position:
        return new ExtractPositionTransformRuleWrapper(extractRule);
      default:
        throw UserException.unsupportedError()
            .message("Unexpected extract rule type: %s", extractRule.getType())
            .build(logger);
    }
  }

  List<ExtractRule> recommendPosition(Selection selection) {
    int start = selection.getOffset();
    int end = start + selection.getLength() - 1;
    int total = selection.getCellText().length();
    List<ExtractRule> rules = new ArrayList<>();
    rules.add(position(new Offset(start, FROM_THE_START), new Offset(end, FROM_THE_START)));
    rules.add(position(new Offset(start, FROM_THE_START), new Offset(total - end - 1, FROM_THE_END)));
    rules.add(position(new Offset(total - start - 1, FROM_THE_END), new Offset(total - end - 1, FROM_THE_END)));
    return rules;
  }

  List<ExtractRule> recommendCharacterGroup(Selection selection) {
    List<ExtractRule> rules = new ArrayList<>();
    String cellText = selection.getCellText();
    int start = selection.getOffset();
    int end = start + selection.getLength();
    String selected = cellText.substring(start, end);
    for (CharType charType : PatternMatchUtils.CharType.values()) {
      boolean startIsCharType = start == 0 ? false : charType.isTypeOf(cellText.charAt(start - 1));
      boolean endIsCharType = end == cellText.length() ? false : charType.isTypeOf(cellText.charAt(end));
      boolean selectionIsCharType = charType.isTypeOf(selected);
      if (!startIsCharType && !endIsCharType && selectionIsCharType) {
        Matcher matcher = charType.matcher(cellText);
        List<Match> matches = PatternMatchUtils.findMatches(matcher, cellText, INDEX);
        for (int index = 0; index < matches.size(); index++) {
          Match match = matches.get(index);
          if (match.start() == start && match.end() == end) {
            rules.add(DatasetsUtil.pattern(charType.pattern(), index, INDEX));
            if (index == matches.size() - 1) {
              rules.add(DatasetsUtil.pattern(charType.pattern(), 0, INDEX_BACKWARDS));
            }
          }
        }
      }
    }
    return rules;
  }

  private static class ExtractPatternTransformRuleWrapper extends TransformRuleWrapper {
    private final ExtractRule rule;

    public ExtractPatternTransformRuleWrapper(ExtractRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      String function = getFunctionExpr(input);
      return String.format("%s IS NOT NULL", function);
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      return getFunction(ExtractPattern.GEN_EXAMPLE, input);
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      return getFunction(ExtractPattern.APPLY, expr);
    }

    private String getFunction(String functionName, String inputExpr) {
      ExtractRulePattern patternRule = rule.getPattern();
      String pattern = patternRule.getPattern();
      if (patternRule.getIgnoreCase() != null && patternRule.getIgnoreCase()) {
        pattern = "(?i)(?u)" + pattern;
      }
      return String.format("%s(%s, %s, %d, %s)",
          functionName,
          inputExpr,
          stringLiteral(pattern),
          patternRule.getIndex(),
          stringLiteral(patternRule.getIndexType().name())
      );
    }

    @Override
    public String describe() {
      return ExtractRecommender.describeExtractRulePattern(rule.getPattern());
    }

    @Override
    public Object getRule() {
      return rule;
    }
  }

  private static class ExtractPositionTransformRuleWrapper extends TransformRuleWrapper {
    private final ExtractRule rule;

    public ExtractPositionTransformRuleWrapper(ExtractRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      return format("length(%s) > 0", getSubStrFunction(input));
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      return format("%s(%s, %d, %s)",
          ExtractPosition.GEN_EXAMPLE,
          input,
          getOffset(rule.getPosition().getStartIndex()),
          getLength(input, rule.getPosition().getStartIndex(), rule.getPosition().getEndIndex())
      );
    }

    @Override
    public String getFunctionExpr(String expr, Object... args) {
      String function = getSubStrFunction(expr);

      // filter out the empty strings. Substr returns empty string when offsets are invalid in input.
      return String.format("CASE WHEN length(%s) > 0 THEN %s ELSE NULL END", function, function);
    }

    private String getSubStrFunction(String inputExpr) {
      ExtractRulePosition position = rule.getPosition();
      return format("substr(%s, %d, %s)", inputExpr,
          getOffset(position.getStartIndex()),
          getLength(inputExpr, position.getStartIndex(), position.getEndIndex()));
    }

    private static int getOffset(Offset start) {
      // Substr takes offset in range [1, length]
      return start.getDirection() == Direction.FROM_THE_END ? -1 * (start.getValue() + 1) : (start.getValue() + 1);
    }

    private static String getLength(String expr, Offset start, Offset end) {
      // Both ends are inclusive
      if (start.getDirection() == Direction.FROM_THE_END) {
        if (end.getDirection() == Direction.FROM_THE_END) {
          // (length(str) - end - 1) - (length(str) - start - 1) + 1 = start - end
          return String.valueOf(start.getValue() - end.getValue() + 1);
        }

        // end - (length(str) - start - 1) + 1 = -length(str) + start + end + 2
        return format("-length(%s) + %d", expr, start.getValue() + end.getValue() + 2);
      }

      if (end.getDirection() == Direction.FROM_THE_END) {
        // (length(str) - end - 1) - start + 1 = length(str) - (start + end)
        return format("length(%s) - %d", expr, start.getValue() + end.getValue());
      }

      // end - start
      return String.valueOf(end.getValue() - start.getValue() + 1);
    }

    @Override
    public String describe() {
      Offset start = rule.getPosition().getStartIndex();
      Offset end = rule.getPosition().getEndIndex();
      return ExtractRecommender.describePlacement(start, end);
    }

    @Override
    public Object getRule() {
      return rule;
    }
  }

  public static String describePlacement(Offset start, Offset end) {
    StringBuilder description = new StringBuilder("Elements: ");
    if (start.getDirection() == end.getDirection()) {
      description.append(start.getValue()).append(" - ").append(end.getValue());
      if (start.getDirection() == FROM_THE_END) {
        description.append(" (both from the end)");
      }
    } else {
      description.append(start.getValue());
      if (start.getDirection() == FROM_THE_END) {
        description.append(" (from the end)");
      }
      description.append(" - ").append(end.getValue());
      if (end.getDirection() == FROM_THE_END) {
        description.append(" (from the end)");
      }
    }
    return description.toString();
  }

  public static String describeExtractRulePattern(ExtractRulePattern extractRulePattern) {
    StringBuilder description = new StringBuilder(extractRulePattern.getPattern());
    switch (extractRulePattern.getIndexType()) {
      case CAPTURE_GROUP:
        if (extractRulePattern.getIndex() == 0) {
          description.append(" first group");
        } else {
          description.append(" group index " + extractRulePattern.getIndex());
        }
        break;
      case INDEX:
        if (extractRulePattern.getIndex() == 0) {
          description.append(" first");
        } else {
          description.append(" index " + extractRulePattern.getIndex());
        }
        break;
      case INDEX_BACKWARDS:
        if (extractRulePattern.getIndex() == 0) {
          description.append(" last");
        } else {
          description.append(" index " + extractRulePattern.getIndex() + " from the end");
        }
        break;
      default:
        throw new UnsupportedOperationException("unknown index type " + extractRulePattern.getIndexType());
    }

    if (extractRulePattern.getIgnoreCase() != null && extractRulePattern.getIgnoreCase()) {
      description.append(" ignore case");
    }

    return description.toString();
  }

  /**
   * Generates a Human readable description from a rule
   * @param rule the rule
   * @return the description
   */
  public static String describe(ExtractRule rule) {
    return DatasetsUtil.accept(rule, new ExtractRuleVisitor<String>() {
      @Override
      public String visit(ExtractRulePattern extractRulePattern) throws Exception {
        return describeExtractRulePattern(extractRulePattern);
      }

      @Override
      public String visit(ExtractRulePosition position) throws Exception {
        Offset start = position.getStartIndex();
        Offset end = position.getEndIndex();
        return describePlacement(start, end);
      }
    });
  }
}
