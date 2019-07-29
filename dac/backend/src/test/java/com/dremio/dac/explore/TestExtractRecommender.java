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

import static com.dremio.dac.explore.PatternMatchUtils.CharType.WORD;
import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_END;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_START;
import static com.dremio.dac.proto.model.dataset.ExtractRuleType.pattern;
import static com.dremio.dac.proto.model.dataset.IndexType.CAPTURE_GROUP;
import static com.dremio.dac.proto.model.dataset.IndexType.INDEX;
import static com.dremio.dac.proto.model.dataset.IndexType.INDEX_BACKWARDS;
import static com.dremio.dac.util.DatasetsUtil.pattern;
import static com.dremio.dac.util.DatasetsUtil.position;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.dac.proto.model.dataset.Direction;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.ExtractRulePattern;
import com.dremio.dac.proto.model.dataset.ExtractRulePosition;
import com.dremio.dac.proto.model.dataset.IndexType;
import com.dremio.dac.proto.model.dataset.Offset;

/**
 * Tests for {@link ExtractRecommender}
 */
public class TestExtractRecommender extends RecommenderTestBase {
  private ExtractRecommender recommender = new ExtractRecommender();

  @Test
  public void ruleSuggestionsSelNumberInTheBeginning() {
    Selection selection = new Selection("col", "883 N Shoreline Blvd., Mountain View, CA 94043", 0, 3);
    List<ExtractRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(5, rules.size());
    comparePattern("\\d+", 0, INDEX, rules.get(0).getPattern());
    comparePattern("\\w+", 0, INDEX, rules.get(1).getPattern());
    comparePos(0, true, 2, true, rules.get(2).getPosition());
    comparePos(0, true, 43, false, rules.get(3).getPosition());
    comparePos(45, false, 43, false, rules.get(4).getPosition());
  }

  @Test
  public void ruleSuggestionsSelNumberAtTheEnd() {
    Selection selection = new Selection("col", "883 N Shoreline Blvd., Mountain View, CA 94043", 41, 5);
    List<ExtractRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(7, rules.size());
    comparePattern("\\d+", 1, INDEX, rules.get(0).getPattern());
    comparePattern("\\d+", 0, INDEX_BACKWARDS, rules.get(1).getPattern());
    comparePattern("\\w+", 7, INDEX, rules.get(2).getPattern());
    comparePattern("\\w+", 0, INDEX_BACKWARDS, rules.get(3).getPattern());
    comparePos(41, true, 45, true, rules.get(4).getPosition());
    comparePos(41, true, 0, false, rules.get(5).getPosition());
    comparePos(4, false, 0, false, rules.get(6).getPosition());
  }

  @Test
  public void ruleSuggestionsSelStateAtTheEnd() {
    Selection selection = new Selection("col", "883 N Shoreline Blvd., Mountain View, CA 94043", 38, 2);
    List<ExtractRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(4, rules.size());
    comparePattern("\\w+", 6, INDEX, rules.get(0).getPattern());
    comparePos(38, true, 39, true, rules.get(1).getPosition());
    comparePos(38, true, 6, false, rules.get(2).getPosition());
    comparePos(7, false, 6, false, rules.get(3).getPosition());
  }

  @Test
  public void emptySelection() {
    boolean exThrown = false;
    try {
      recommender.getRules(new Selection("col", "883 N Shoreline Blvd.", 5, 0), TEXT);
      fail("not expected to reach here");
    } catch (UserException e) {
      exThrown = true;
      assertEquals("text recommendation requires non-empty text selection", e.getMessage());
    }

    assertTrue("expected a UserException", exThrown);
  }

  private void comparePos(int start, boolean sFromBegin, int end, boolean eFromBegin, ExtractRulePosition rule) {
    assertEquals(start, rule.getStartIndex().getValue().intValue());
    assertEquals(sFromBegin, rule.getStartIndex().getDirection() == Direction.FROM_THE_START);
    assertEquals(end, rule.getEndIndex().getValue().intValue());
    assertEquals(eFromBegin, rule.getEndIndex().getDirection() == Direction.FROM_THE_START);
  }

  private void comparePattern(String pattern, int index, IndexType indexType, ExtractRulePattern rule) {
    assertEquals(pattern, rule.getPattern());
    assertEquals(index, rule.getIndex().intValue());
    assertEquals(indexType, rule.getIndexType());
  }

  @Test
  public void testRecommendCharacterGroup() {
    List<ExtractRule> cards = recommender.recommendCharacterGroup(new Selection("col", "abc def,ghi", 4, 3));
    assertEquals(1, cards.size());
    assertEquals(pattern, cards.get(0).getType());
    ExtractRulePattern pattern = cards.get(0).getPattern();
    assertEquals(1, pattern.getIndex().intValue());
    assertEquals(WORD.pattern(), pattern.getPattern());
    assertEquals(INDEX, pattern.getIndexType());
  }

  @Test
  public void testExtractPatternExprs() throws Exception {
    File file = temp.newFile("extractTextPattern.json");
    try (PrintWriter writer = new PrintWriter(file)) {
      writer.write(
        "{ \"col\" : \"aaa.bbb.ccc.ddd\" }\n" +
          "{ \"col\" : \"ABC.dfa.dsff\" }\n" +
          "{ \"col\" : \"def3er\" }\n" +
          "{ \"col\" : \"tyu\" }\n" +
          "{ \"col\" : null }"
      );
    }
    {
      ExtractRule rule = pattern("\\d+12", 0, INDEX);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '\\d+12', 0, 'INDEX') IS NOT NULL", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '\\d+12', 0, 'INDEX')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '\\d+12', 0, 'INDEX')", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("\\d+12 first", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list(null, null, null, null, null),
        list(false, false, false, false, false),
        asList((List<CardExamplePosition>) null, null, null, null, null)
      );
    }
    {
      ExtractRule rule = pattern("\\d+", 0, INDEX_BACKWARDS);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '\\d+', 0, 'INDEX_BACKWARDS') IS NOT NULL", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '\\d+', 0, 'INDEX_BACKWARDS')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '\\d+', 0, 'INDEX_BACKWARDS')", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("\\d+ last", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) null, null, "3", null, null),
        list(false, false, true, false, false),
        asList(null, null, asList(new CardExamplePosition(3, 1)), null, null)
      );
    }
    {
      ExtractRule rule = pattern("\\w+", 1, INDEX);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '\\w+', 1, 'INDEX') IS NOT NULL", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '\\w+', 1, 'INDEX')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '\\w+', 1, 'INDEX')", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("\\w+ index 1", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "bbb", "dfa", null, null, null),
        list(true, true, false, false, false),
        asList(asList(new CardExamplePosition(4, 3)), asList(new CardExamplePosition(4, 3)), null, null, null)
      );
    }
    {
      ExtractRule rule = pattern("(\\w+)\\.(\\w+)\\.(\\w+)\\..*", 1, CAPTURE_GROUP);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 1, 'CAPTURE_GROUP') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 1, 'CAPTURE_GROUP')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 1, 'CAPTURE_GROUP')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("(\\w+)\\.(\\w+)\\.(\\w+)\\..* group index 1", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "bbb", null, null, null, null),
        list(true, false, false, false, false),
        asList(asList(new CardExamplePosition(4, 3)), null, null, null, null)
      );
    }
    {
      ExtractRule rule = pattern("(\\w+)\\.(\\w+)\\.(\\w+)\\..*", 2, CAPTURE_GROUP);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 2, 'CAPTURE_GROUP') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 2, 'CAPTURE_GROUP')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '(\\w+)\\.(\\w+)\\.(\\w+)\\..*', 2, 'CAPTURE_GROUP')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("(\\w+)\\.(\\w+)\\.(\\w+)\\..* group index 2", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "ccc", null, null, null, null),
        list(true, false, false, false, false),
        asList(asList(new CardExamplePosition(8, 3)), null, null, null, null)
      );
    }
    {
      ExtractRule rule = pattern("(\\w+)\\.(\\w+)\\..*", 0, CAPTURE_GROUP);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\..*', 0, 'CAPTURE_GROUP') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '(\\w+)\\.(\\w+)\\..*', 0, 'CAPTURE_GROUP')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '(\\w+)\\.(\\w+)\\..*', 0, 'CAPTURE_GROUP')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("(\\w+)\\.(\\w+)\\..* first group", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "aaa", "ABC", null, null, null),
        list(true, true, false, false, false),
        asList(asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(0, 3)), null, null, null)
      );
    }
    {
      ExtractRule rule = pattern(".*(\\d+).*", 0, CAPTURE_GROUP);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '.*(\\d+).*', 0, 'CAPTURE_GROUP') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '.*(\\d+).*', 0, 'CAPTURE_GROUP')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '.*(\\d+).*', 0, 'CAPTURE_GROUP')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals(".*(\\d+).* first group", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) null, null, "3", null, null),
        list(false, false, true, false, false),
        asList(null, null, asList(new CardExamplePosition(3, 1)), null, null)
      );
    }
    {
      ExtractRule rule = pattern("[a-z]{3}", 0, INDEX);
      rule.getPattern().setIgnoreCase(true);
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '(?i)(?u)[a-z]{3}', 0, 'INDEX') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '(?i)(?u)[a-z]{3}', 0, 'INDEX')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '(?i)(?u)[a-z]{3}', 0, 'INDEX')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("[a-z]{3} first ignore case", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "aaa", "ABC", "def", "tyu", null),
        list(true, true, true, true, false),
        asList(asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(0, 3)), null)
      );
    }
    {
      ExtractRule rule = pattern("[a-z]{3}", 0, INDEX); // ignore case false by default
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("extract_pattern(tbl.foo, '[a-z]{3}', 0, 'INDEX') IS NOT NULL",
        wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern(tbl.foo, '[a-z]{3}', 0, 'INDEX')", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_pattern_example(tbl.foo, '[a-z]{3}', 0, 'INDEX')",
        wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("[a-z]{3} first", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "aaa", "dfa", "def", "tyu", null),
        list(true, true, true, true, false),
        asList(asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(4, 3)), asList(new CardExamplePosition(0, 3)), asList(new CardExamplePosition(0, 3)), null)
      );
    }
  }

  @Test
  public void testExtractPositionExprs() throws Exception {
    File file = temp.newFile("extractTextPos.json");
    try (PrintWriter writer = new PrintWriter(file)) {
      writer.write(
        "{ \"col\" : \"aabbbcccc\" }\n" +
          "{ \"col\" : \"ddde\" }\n" +
          "{ \"col\" : \"fffff\" }\n" +
          "{ \"col\" : null }"
      );
    }
    {
      ExtractRule rule = position(new Offset(1, FROM_THE_START), new Offset(4, FROM_THE_START));
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("length(substr(tbl.foo, 2, 4)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("CASE WHEN length(substr(tbl.foo, 2, 4)) > 0 THEN substr(tbl.foo, 2, 4) ELSE NULL END", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_position_example(tbl.foo, 2, 4)", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 4", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "abbb", "dde", "ffff", null),
        list(true, true, true, false),
        asList(asList(new CardExamplePosition(1, 4)), asList(new CardExamplePosition(1, 3)), asList(new CardExamplePosition(1, 4)), null)
      );
    }
    {
      ExtractRule rule = position(new Offset(4, FROM_THE_END), new Offset(2, FROM_THE_END));
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("length(substr(tbl.foo, -5, 3)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("CASE WHEN length(substr(tbl.foo, -5, 3)) > 0 THEN substr(tbl.foo, -5, 3) ELSE NULL END", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_position_example(tbl.foo, -5, 3)", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("Elements: 4 - 2 (both from the end)", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "bcc", null, "fff", null),
        list(true, false, true, false),
        asList(asList(new CardExamplePosition(4, 3)), null, asList(new CardExamplePosition(0, 3)), null)
      );
    }
    {
      ExtractRule rule = position(new Offset(1, FROM_THE_START), new Offset(1, FROM_THE_END));
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("length(substr(tbl.foo, 2, length(tbl.foo) - 2)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("CASE WHEN length(substr(tbl.foo, 2, length(tbl.foo) - 2)) > 0 THEN substr(tbl.foo, 2, length(tbl.foo) - 2) ELSE NULL END",
        wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_position_example(tbl.foo, 2, length(tbl.foo) - 2)", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 1 (from the end)", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "abbbccc", "dd", "fff", null),
        list(true, true, true, false),
        asList(asList(new CardExamplePosition(1, 7)), asList(new CardExamplePosition(1, 2)), asList(new CardExamplePosition(1, 3)), null)
      );
    }
    {
      ExtractRule rule = position(new Offset(4, FROM_THE_END), new Offset(4, FROM_THE_START));
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("length(substr(tbl.foo, -5, -length(tbl.foo) + 10)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("CASE WHEN length(substr(tbl.foo, -5, -length(tbl.foo) + 10)) > 0 THEN substr(tbl.foo, -5, -length(tbl.foo) + 10) ELSE NULL END",
        wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_position_example(tbl.foo, -5, -length(tbl.foo) + 10)", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("Elements: 4 (from the end) - 4", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "b", null, "fffff", null),
        list(true, false, true, false),
        null
      );
    }
  }

  @Test
  public void testExtractPositionExample() throws Exception {
    File file = temp.newFile("extractTextPosExample.json");
    try (PrintWriter writer = new PrintWriter(file)) {
      writer.write(
        "{ \"col\" : \"aabbbcccc\" }\n" +
          "{ \"col\" : \"ddde\" }\n" +
          "{ \"col\" : \"fffff\" }\n" +
          "{ \"col\" : null }"
      );
    }
    {
      ExtractRule rule = position(new Offset(1, FROM_THE_START), new Offset(4, FROM_THE_START));
      TransformRuleWrapper<ExtractRule> wrapper = recommender.wrapRule(rule);

      assertEquals("length(substr(tbl.foo, 2, 4)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("CASE WHEN length(substr(tbl.foo, 2, 4)) > 0 THEN substr(tbl.foo, 2, 4) ELSE NULL END", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("extract_position_example(tbl.foo, 2, 4)", wrapper.getExampleFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 4", wrapper.describe());
      validate(file.getAbsolutePath(), wrapper,
        new Object[0], list((Object) "ffff"),
        list(true),
        asList(asList(new CardExamplePosition(1, 4))),
        "WHERE col = 'fffff'"
      );
    }
  }
}
