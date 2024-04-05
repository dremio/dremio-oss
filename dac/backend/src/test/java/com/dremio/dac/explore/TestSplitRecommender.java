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

import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.dac.proto.model.dataset.MatchType;
import com.dremio.dac.proto.model.dataset.SplitPositionType;
import com.dremio.dac.proto.model.dataset.SplitRule;
import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import org.junit.Test;

/** Tests for {@link SplitRecommender} */
public class TestSplitRecommender extends RecommenderTestBase {
  private SplitRecommender recommender = new SplitRecommender();

  @Test
  public void ruleSuggestionsAlphabetCharsDelimiter() {
    // Select text containing alphabet characters as delimiter
    Selection selection = new Selection("col", "abbbabbabbaaa", 1, 2); // "bb" is selected
    List<SplitRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(2, rules.size());
    compare(MatchType.exact, "bb", false, rules.get(0));
    compare(MatchType.exact, "bb", true, rules.get(1));
  }

  @Test
  public void ruleSuggestionsPipeDelimiter() {
    // Select text containing non-alphabet characters as delimiter
    Selection selection = new Selection("col", "1|2|3|4|5", 1, 1); // "|" is selected
    List<SplitRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(1, rules.size());
    compare(MatchType.exact, "|", false, rules.get(0));
  }

  private void compare(MatchType matchType, String pattern, boolean ignoreCase, SplitRule rule) {
    assertEquals(matchType, rule.getMatchType());
    assertEquals(pattern, rule.getPattern());
    assertEquals(ignoreCase, rule.getIgnoreCase() != null && rule.getIgnoreCase());
  }

  @Test
  public void splitListCardGen() throws Exception {
    File dataFile = temp.newFile("splitExact.json");
    try (PrintWriter writer = new PrintWriter(dataFile)) {
      writer.append("{ \"col\" : \"aBa\" }");
      writer.append("{ \"col\" : \"abaBa\" }");
      writer.append("{ \"col\" : null }");
      writer.append("{ \"col\" : \"abababa\" }");
      writer.append("{ \"col\" : \"aaa\" }");
    }

    {
      SplitRule rule = new SplitRule("b", MatchType.exact, true);
      TransformRuleWrapper<SplitRule> wrapper = recommender.wrapRule(rule);

      assertEquals(
          "regexp_matches(\"tbl name\".col, '(?i)(?u).*\\Qb\\E.*')",
          wrapper.getMatchFunctionExpr("\"tbl name\".col"));
      assertEquals(
          "regexp_split(tbl.col, '(?i)(?u)\\Qb\\E', 'ALL', 10)",
          wrapper.getFunctionExpr("tbl.col", SplitPositionType.ALL, 10));
      assertEquals(
          "regexp_split_positions(tbl.\"col name\", '(?i)(?u)\\Qb\\E')",
          wrapper.getExampleFunctionExpr("tbl.\"col name\""));
      assertEquals("Exactly matches \"b\" ignore case", wrapper.describe());

      List<List<CardExamplePosition>> highlights =
          asList(
              asList(new CardExamplePosition(1, 1)),
              asList(new CardExamplePosition(1, 1), new CardExamplePosition(3, 1)),
              null,
              asList(
                  new CardExamplePosition(1, 1),
                  new CardExamplePosition(3, 1),
                  new CardExamplePosition(5, 1)),
              null);
      validate(
          dataFile.getAbsolutePath(),
          wrapper,
          new Object[] {SplitPositionType.INDEX, 1},
          list((Object) list("aBa"), list("aba", "a"), null, list("aba", "aba"), list("aaa")),
          asList(true, true, false, true, false),
          highlights);
    }
    {
      SplitRule rule = new SplitRule("b", MatchType.exact, false);
      TransformRuleWrapper<SplitRule> wrapper = recommender.wrapRule(rule);

      assertEquals(
          "regexp_matches(tbl.col, '.*\\Qb\\E.*')", wrapper.getMatchFunctionExpr("tbl.col"));
      assertEquals(
          "regexp_split(tbl.col, '\\Qb\\E', 'ALL', 10)",
          wrapper.getFunctionExpr("tbl.col", SplitPositionType.ALL, 10));
      assertEquals(
          "regexp_split_positions(tbl.col, '\\Qb\\E')", wrapper.getExampleFunctionExpr("tbl.col"));
      assertEquals("Exactly matches \"b\"", wrapper.describe());

      List<List<CardExamplePosition>> highlights =
          asList(
              null,
              asList(new CardExamplePosition(1, 1)),
              null,
              asList(
                  new CardExamplePosition(1, 1),
                  new CardExamplePosition(3, 1),
                  new CardExamplePosition(5, 1)),
              null);

      validate(
          dataFile.getAbsolutePath(),
          wrapper,
          new Object[] {SplitPositionType.LAST, -1},
          list((Object) list("aBa"), list("a", "aBa"), null, list("ababa", "a"), list("aaa")),
          asList(false, true, false, true, false),
          highlights);
    }
  }

  @Test
  public void splitRegex() throws Exception {
    File dataFile = temp.newFile("splitRegex.json");
    try (PrintWriter writer = new PrintWriter(dataFile)) {
      writer.append("{ \"col\" : \"aaa111BBB222cc33d\" }");
      writer.append("{ \"col\" : \"a1b2c3\" }");
      writer.append("{ \"col\" : null }");
      writer.append("{ \"col\" : \"abababa\" }");
      writer.append("{ \"col\" : \"111\" }");
    }

    {
      SplitRule rule = new SplitRule("\\d+", MatchType.regex, false);
      TransformRuleWrapper<SplitRule> wrapper = recommender.wrapRule(rule);

      assertEquals(
          "regexp_matches(\"tbl name\".col, '.*\\d+.*')",
          wrapper.getMatchFunctionExpr("\"tbl name\".col"));
      assertEquals(
          "regexp_split(tbl.col, '\\d+', 'ALL', 10)",
          wrapper.getFunctionExpr("tbl.col", SplitPositionType.ALL, 10));
      assertEquals(
          "regexp_split_positions(tbl.\"col name\", '\\d+')",
          wrapper.getExampleFunctionExpr("tbl.\"col name\""));
      assertEquals("Matches regex \"\\d+\"", wrapper.describe());

      List<List<CardExamplePosition>> highlights =
          asList(
              asList(
                  new CardExamplePosition(3, 3),
                  new CardExamplePosition(9, 3),
                  new CardExamplePosition(14, 2)),
              asList(
                  new CardExamplePosition(1, 1),
                  new CardExamplePosition(3, 1),
                  new CardExamplePosition(5, 1)),
              null,
              null,
              asList(new CardExamplePosition(0, 3)));

      validate(
          dataFile.getAbsolutePath(),
          wrapper,
          new Object[] {SplitPositionType.FIRST, -1},
          list(
              (Object) list("aaa", "BBB222cc33d"),
              list("a", "b2c3"),
              null,
              list("abababa"),
              list("", "")),
          asList(true, true, false, false, true),
          highlights);
    }
    {
      SplitRule rule = new SplitRule("[a-z]+", MatchType.regex, true);
      TransformRuleWrapper<SplitRule> wrapper = recommender.wrapRule(rule);

      assertEquals(
          "regexp_matches(tbl.col, '(?i)(?u).*[a-z]+.*')", wrapper.getMatchFunctionExpr("tbl.col"));
      assertEquals(
          "regexp_split(tbl.col, '(?i)(?u)[a-z]+', 'ALL', 10)",
          wrapper.getFunctionExpr("tbl.col", SplitPositionType.ALL, 10));
      assertEquals(
          "regexp_split_positions(tbl.col, '(?i)(?u)[a-z]+')",
          wrapper.getExampleFunctionExpr("tbl.col"));
      assertEquals("Matches regex \"[a-z]+\" ignore case", wrapper.describe());

      List<List<CardExamplePosition>> highlights =
          asList(
              asList(
                  new CardExamplePosition(0, 3),
                  new CardExamplePosition(6, 3),
                  new CardExamplePosition(12, 2),
                  new CardExamplePosition(16, 1)),
              asList(
                  new CardExamplePosition(0, 1),
                  new CardExamplePosition(2, 1),
                  new CardExamplePosition(4, 1)),
              null,
              asList(new CardExamplePosition(0, 7)),
              null);

      validate(
          dataFile.getAbsolutePath(),
          wrapper,
          new Object[] {SplitPositionType.ALL, 2},
          list((Object) list("", "111"), list("", "1"), null, list("", ""), list("111")),
          asList(true, true, false, true, false),
          highlights);
    }
  }
}
