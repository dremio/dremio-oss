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
package com.dremio.dac.explore;

import static com.dremio.common.utils.SqlUtils.stringLiteral;
import static com.dremio.dac.proto.model.dataset.IndexType.INDEX;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.CONTAINS;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.ENDS_WITH;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.EXACT;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.IS_NULL;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.MATCHES;
import static com.dremio.dac.proto.model.dataset.ReplaceSelectionType.STARTS_WITH;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.regex.Pattern.quote;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.PatternMatchUtils.Match;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.explore.udfs.MatchPattern;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;
import com.dremio.dac.proto.model.dataset.ReplaceType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replace/Keep only/Exclude transformation recommendation suggestions and generating examples and
 * number of matches in sample data for each recommendation.
 */
public class ReplaceRecommender extends Recommender<ReplacePatternRule, Selection> {
  private static final Logger logger = LoggerFactory.getLogger(ReplaceRecommender.class);

  @Override
  public List<ReplacePatternRule> getRules(Selection selection, DataType selColType) {
    checkArgument(
        selColType == DataType.TEXT,
        "Cards generation for Replace/Keeponly/Exclude transforms is supported only on TEXT type columns");

    List<ReplacePatternRule> rules = new ArrayList<>();
    if (selection.getCellText() == null) {
      rules.add(new ReplacePatternRule(IS_NULL));
    } else {
      final int start = selection.getOffset();
      final int end = start + selection.getLength();
      final String content = selection.getCellText().substring(start, end);

      // Applicable only for text type
      if (content.length() > 0) {
        rules.addAll(casePermutations(CONTAINS, content));
        if (start == 0) {
          rules.addAll(casePermutations(STARTS_WITH, content));
        }
        if (end == selection.getCellText().length()) {
          rules.addAll(casePermutations(ENDS_WITH, content));
        }
        List<String> patterns = recommendReplacePattern(selection);
        for (String pattern : patterns) {
          rules.add(new ReplacePatternRule(MATCHES).setSelectionPattern(pattern));
        }
      }

      if (start == 0 && end == selection.getCellText().length()) {
        rules.addAll(casePermutations(EXACT, content));
      }
    }

    return rules;
  }

  private List<ReplacePatternRule> casePermutations(
      ReplaceSelectionType selectionType, String pattern) {
    /**
     * Add case permutations of the rule if the selected text/pattern has no case (such as numbers)
     */
    if (!pattern.toUpperCase().equals(pattern.toLowerCase())) {
      return Arrays.asList(
          new ReplacePatternRule(selectionType).setSelectionPattern(pattern).setIgnoreCase(true),
          new ReplacePatternRule(selectionType).setSelectionPattern(pattern).setIgnoreCase(false));
    }

    return Collections.singletonList(
        new ReplacePatternRule(selectionType).setSelectionPattern(pattern).setIgnoreCase(false));
  }

  private List<String> recommendReplacePattern(Selection selection) {
    List<String> rules = new ArrayList<>();
    String cellText = selection.getCellText();
    int start = selection.getOffset();
    int end = start + selection.getLength();
    String selected = cellText.substring(start, end);
    boolean startIsCharType =
        start == 0 ? false : PatternMatchUtils.CharType.DIGIT.isTypeOf(cellText.charAt(start - 1));
    boolean endIsCharType =
        end == cellText.length()
            ? false
            : PatternMatchUtils.CharType.DIGIT.isTypeOf(cellText.charAt(end));
    boolean selectionIsCharType = PatternMatchUtils.CharType.DIGIT.isTypeOf(selected);
    if (!startIsCharType && !endIsCharType && selectionIsCharType) {
      Matcher matcher = PatternMatchUtils.CharType.DIGIT.matcher(cellText);
      List<Match> matches = PatternMatchUtils.findMatches(matcher, cellText, INDEX);
      for (int index = 0; index < matches.size(); index++) {
        Match match = matches.get(index);
        if (match.start() == start && match.end() == end) {
          rules.add(PatternMatchUtils.CharType.DIGIT.pattern());
        }
      }
    }
    return rules;
  }

  @Override
  public TransformRuleWrapper<ReplacePatternRule> wrapRule(ReplacePatternRule rule) {
    return new ReplaceTransformRuleWrapper(rule);
  }

  private static class ReplaceTransformRuleWrapper extends Recommender.TransformRuleWrapper {
    private final ReplacePatternRule rule;

    ReplaceTransformRuleWrapper(ReplacePatternRule rule) {
      this.rule = rule;
    }

    @Override
    public String getMatchFunctionExpr(String input) {
      return getMatchFunction(input);
    }

    @Override
    public boolean canGenerateExamples() {
      return rule.getSelectionType() != IS_NULL;
    }

    @Override
    public String getExampleFunctionExpr(String input) {
      final boolean ignoreCase = rule.getIgnoreCase() == null ? false : rule.getIgnoreCase();
      final String quotedPattern = stringLiteral(rule.getSelectionPattern());

      return String.format(
          "%s(%s, '%s', %s, %s)",
          MatchPattern.GEN_EXAMPLE,
          input,
          rule.getSelectionType().toString(),
          quotedPattern,
          ignoreCase);
    }

    private String getMatchFunction(String expr) {
      // Generate the match function
      if (rule.getSelectionType() == IS_NULL) {
        return String.format("%s IS NULL", expr);
      } else if (rule.getSelectionType() == EXACT) {
        if (rule.getIgnoreCase() != null && rule.getIgnoreCase()) {
          return String.format(
              "lower(%s) = lower(%s)", expr, stringLiteral(rule.getSelectionPattern()));
        } else {
          return String.format("%s = %s", expr, stringLiteral(rule.getSelectionPattern()));
        }
      } else {
        String patternLiteral = getRegexPatternLiteral(true);
        return String.format("regexp_like(%s, %s)", expr, patternLiteral);
      }
    }

    @Override
    public String getFunctionExpr(String inputExpr, Object... args) {
      checkArgument(
          args.length >= 1 && args[0] != null, "Expected the replace type as first argument");
      ReplaceType replaceType = (ReplaceType) args[0];

      String replacementValue = null;
      if (replaceType != ReplaceType.NULL) {
        checkArgument(
            args.length == 2 && args[1] != null,
            "Expected the replacement value as second argument");
        replacementValue = stringLiteral((String) args[1]);
      }

      String matchExpr = getMatchFunction(inputExpr);
      switch (replaceType) {
        case NULL:
          return String.format("CASE WHEN %s THEN NULL ELSE %s END", matchExpr, inputExpr);
        case VALUE:
          return String.format(
              "CASE WHEN %s THEN %s ELSE %s END", matchExpr, replacementValue, inputExpr);
        case SELECTION:
          String replace;
          switch (rule.getSelectionType()) {
            case IS_NULL:
            case EXACT:
              replace = replacementValue;
              break;
            case CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
            case MATCHES:
              replace =
                  String.format(
                      "regexp_replace(%s, %s, %s)",
                      inputExpr, getRegexPatternLiteral(false), replacementValue);
              break;
            default:
              throw UserException.unsupportedError()
                  .message("unsupported selection type: " + rule.getSelectionType().toString())
                  .build(logger);
          }

          return String.format("CASE WHEN %s THEN %s ELSE %s END", matchExpr, replace, inputExpr);
        default:
          throw UserException.unsupportedError()
              .message("Unsupported replace type: " + replaceType.toString())
              .build(logger);
      }
    }

    @Override
    public ReplacePatternRule getRule() {
      return rule;
    }

    private String getRegexPatternLiteral(boolean forMatch) {
      final String patternLiteral = rule.getSelectionPattern();
      String regexp;
      switch (rule.getSelectionType()) {
        case CONTAINS:
          regexp = regexp(forMatch) + quote(patternLiteral) + regexp(forMatch);
          break;
        case STARTS_WITH:
          regexp = "^" + quote(patternLiteral) + regexp(forMatch);
          break;
        case ENDS_WITH:
          regexp = regexp(forMatch) + quote(patternLiteral) + "$";
          break;
        case MATCHES:
          regexp = regexp(forMatch) + patternLiteral + regexp(forMatch);
          break;
        default:
          throw UserException.unsupportedError()
              .message(
                  "regexp not available for selection type: " + rule.getSelectionType().toString())
              .build(logger);
      }

      if (rule.getIgnoreCase() != null && rule.getIgnoreCase()) {
        // make the pattern case insensitive and unicode case aware
        regexp = "(?i)(?u)" + regexp;
      }

      return stringLiteral(regexp);
    }

    private static String regexp(boolean forMatch) {
      return forMatch ? ".*?" : "";
    }

    @Override
    public String describe() {
      String suffix = (rule.getIgnoreCase() != null && rule.getIgnoreCase()) ? " ignore case" : "";
      switch (rule.getSelectionType()) {
        case CONTAINS:
          return String.format("Contains %s", rule.getSelectionPattern()) + suffix;
        case ENDS_WITH:
          return String.format("Ends with %s", rule.getSelectionPattern()) + suffix;
        case EXACT:
          return String.format("Exactly matches %s", rule.getSelectionPattern()) + suffix;
        case IS_NULL:
          return "Is null";
        case MATCHES:
          return String.format("Matches regex %s", rule.getSelectionPattern()) + suffix;
        case STARTS_WITH:
          return String.format("Starts with %s", rule.getSelectionPattern()) + suffix;
        default:
          throw new UnsupportedOperationException(rule.getSelectionType().name());
      }
    }
  }

  public static ReplaceMatcher getMatcher(ReplacePatternRule rule) {
    final String pattern = rule.getSelectionPattern();
    ReplaceSelectionType selectionType = rule.getSelectionType();
    if (rule.getIgnoreCase() != null && rule.getIgnoreCase()) {
      return new ToLowerReplaceMatcher(getMatcher(pattern.toLowerCase(), selectionType));
    } else {
      return getMatcher(pattern, selectionType);
    }
  }

  private static ReplaceMatcher getMatcher(
      final String pattern, ReplaceSelectionType selectionType) {
    switch (selectionType) {
      case CONTAINS:
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value != null && value.contains(pattern)) {
              int start = value.indexOf(pattern);
              int end = start + pattern.length();
              return new Match(start, end);
            }
            return null;
          }
        };
      case ENDS_WITH:
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value != null && value.endsWith(pattern)) {
              int start = value.length() - pattern.length();
              int end = value.length();
              return new Match(start, end);
            }
            return null;
          }
        };
      case EXACT:
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value != null && value.equals(pattern)) {
              int start = 0;
              int end = value.length();
              return new Match(start, end);
            }
            return null;
          }
        };
      case IS_NULL:
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value == null) {
              int start = 0;
              int end = 0;
              return new Match(start, end);
            }
            return null;
          }
        };
      case MATCHES:
        final Pattern patternC = Pattern.compile(pattern);
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value != null) {
              Matcher matcher = patternC.matcher(value);
              if (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                return new Match(start, end);
              }
            }
            return null;
          }
        };
      case STARTS_WITH:
        return new ReplaceMatcher() {
          @Override
          public Match matches(String value) {
            if (value != null && value.startsWith(pattern)) {
              int start = 0;
              int end = pattern.length();
              return new Match(start, end);
            }
            return null;
          }
        };
      default:
        throw new UnsupportedOperationException(selectionType.name());
    }
  }

  /** Selection matcher for replace */
  public abstract static class ReplaceMatcher {
    public abstract Match matches(String value);
  }

  /** turns all input to lower case */
  public static class ToLowerReplaceMatcher extends ReplaceMatcher {

    private final ReplaceMatcher delegate;

    public ToLowerReplaceMatcher(ReplaceMatcher delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public Match matches(String value) {
      return delegate.matches(value == null ? value : value.toLowerCase());
    }
  }
}
