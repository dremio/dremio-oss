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

import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import org.junit.Test;

import com.dremio.dac.explore.PatternMatchUtils.Match;
import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.ReplaceRecommender.ReplaceMatcher;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.CardExamplePosition;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;
import com.dremio.dac.proto.model.dataset.ReplaceType;

/**
 * Tests for {@link ReplaceRecommender}
 */
public class TestReplaceRecommender extends RecommenderTestBase {
  private ReplaceRecommender recommender = new ReplaceRecommender();

  @Test
  public void testMatcher() {
    ReplaceMatcher matcher = ReplaceRecommender.getMatcher(
        new ReplacePatternRule(ReplaceSelectionType.MATCHES)
            .setSelectionPattern("[ac]")
            .setIgnoreCase(false));
    Match match = matcher.matches("abc");
    assertNotNull(match);
    assertEquals("Match(0, 1)", match.toString());
  }

  @Test
  public void ruleSuggestionsSelectMiddleText() {
    // Select some text in the middle of cell
    Selection selection = new Selection("col", "This is a text", 5, 2); // "is" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(2, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "is", true, rules.get(0));
    compare(ReplaceSelectionType.CONTAINS, "is", false, rules.get(1));
  }

  @Test
  public void ruleSuggestionsSelectMiddleNumber() {
    // Select a number within a text type cell
    Selection selection = new Selection("col", "There are 20 types of people", 10, 2); // "20" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(2, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "20", false, rules.get(0));
    compare(ReplaceSelectionType.MATCHES, "\\d+", false, rules.get(1));
  }

  @Test
  public void ruleSuggestionsSelectBeginningText() {
    // Select beginning text in cell
    Selection selection = new Selection("col", "There are 20 types of people", 0, 5); // "There" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(4, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "There", true, rules.get(0));
    compare(ReplaceSelectionType.CONTAINS, "There", false, rules.get(1));
    compare(ReplaceSelectionType.STARTS_WITH, "There", true, rules.get(2));
    compare(ReplaceSelectionType.STARTS_WITH, "There", false, rules.get(3));
  }

  @Test
  public void ruleSuggestionsSelectEndingText() {
    // Select ending text in cell
    Selection selection = new Selection("col", "There are 20 types of people", 22, 6); // "people" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(4, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "people", true, rules.get(0));
    compare(ReplaceSelectionType.CONTAINS, "people", false, rules.get(1));
    compare(ReplaceSelectionType.ENDS_WITH, "people", true, rules.get(2));
    compare(ReplaceSelectionType.ENDS_WITH, "people", false, rules.get(3));
  }

  @Test
  public void ruleSuggestionsSelectAllText() {
    // Select all the text in text type cell
    Selection selection = new Selection("col", "There are 20 types of people", 0, 28); // everything selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(8, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "There are 20 types of people", true, rules.get(0));
    compare(ReplaceSelectionType.CONTAINS, "There are 20 types of people", false, rules.get(1));
    compare(ReplaceSelectionType.STARTS_WITH, "There are 20 types of people", true, rules.get(2));
    compare(ReplaceSelectionType.STARTS_WITH, "There are 20 types of people", false, rules.get(3));
    compare(ReplaceSelectionType.ENDS_WITH, "There are 20 types of people", true, rules.get(4));
    compare(ReplaceSelectionType.ENDS_WITH, "There are 20 types of people", false, rules.get(5));
    compare(ReplaceSelectionType.EXACT, "There are 20 types of people", true, rules.get(6));
    compare(ReplaceSelectionType.EXACT, "There are 20 types of people", false, rules.get(7));
  }

  @Test
  public void ruleSuggestionsSelectNumberAtTheEnd() {
    // Select number at the end of the cell text
    Selection selection = new Selection("col", "883 N Shoreline Blvd, Mountain View, CA 94043", 40, 5); // "94043" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(3, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "94043", false, rules.get(0));
    compare(ReplaceSelectionType.ENDS_WITH, "94043", false, rules.get(1));
    compare(ReplaceSelectionType.MATCHES, "\\d+", false, rules.get(2));
  }

  @Test
  public void ruleSuggestionsSelectNumberAtBeginning() {
    // Select number beginning of the cell text
    Selection selection = new Selection("col", "883 N Shoreline Blvd, Mountain View, CA 94043", 0, 3); // "883" is selected
    List<ReplacePatternRule> rules = recommender.getRules(selection, TEXT);
    assertEquals(3, rules.size());
    compare(ReplaceSelectionType.CONTAINS, "883", false, rules.get(0));
    compare(ReplaceSelectionType.STARTS_WITH, "883", false, rules.get(1));
    compare(ReplaceSelectionType.MATCHES, "\\d+", false, rules.get(2));
  }

  private void compare(ReplaceSelectionType type, String pattern, boolean ignoreCase, ReplacePatternRule r) {
    assertEquals(type, r.getSelectionType());
    assertEquals(pattern, r.getSelectionPattern());
    assertEquals(ignoreCase, r.getIgnoreCase() == null ? false : r.getIgnoreCase());
  }

  @Test
  public void contains() throws Exception {
    final File dataFile = temp.newFile("containsTest.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"This is an amazing restaurant. Very good food.\" }");
      printWriter.append("{ \"col\" : \"This is a worst restaurant. bad food.\" }");
      printWriter.append("{ \"col\" : \"Amazing environment. \\nAmazing food.\" }");
      printWriter.append("{ \"col\" : \"Not good.\" }");
    }

    ReplacePatternRule rule = new ReplacePatternRule(ReplaceSelectionType.CONTAINS)
        .setSelectionPattern("amazing");
    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'CONTAINS', 'amazing', false)", // example
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qamazing\\E.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qamazing\\E.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '\\Qamazing\\E', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qamazing\\E.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '.*?\\Qamazing\\E.*?')" // match expression
    );

    List<Object> outputSelReplace = list(
        (Object)"This is an AMAZING restaurant. Very good food.",
        "This is a worst restaurant. bad food.",
        "Amazing environment. \nAmazing food.",
        "Not good."
    );

    List<Object> outputCompleteReplace = list(
        (Object)"AMAZING",
        "This is a worst restaurant. bad food.",
        "Amazing environment. \nAmazing food.",
        "Not good."
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "AMAZING"}, outputSelReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(11, 7)), null, null, null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "AMAZING"}, outputCompleteReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(11, 7)), null, null, null)
    );

    // with ignore case set to false
    rule = rule.setIgnoreCase(true);
    ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'CONTAINS', 'amazing', true)", // example
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qamazing\\E.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qamazing\\E.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '(?i)(?u)\\Qamazing\\E', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qamazing\\E.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '(?i)(?u).*?\\Qamazing\\E.*?')" // match expression
    );

    outputSelReplace = list(
        (Object)"This is an AMAZING restaurant. Very good food.",
        "This is a worst restaurant. bad food.",
        "AMAZING environment. \nAMAZING food.",
        "Not good."
    );

    outputCompleteReplace = list(
        (Object)"AMAZING",
        "This is a worst restaurant. bad food.",
        "AMAZING",
        "Not good."
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "AMAZING"}, outputSelReplace,
        asList(true, false, true, false),
        asList(asList(new CardExamplePosition(11, 7)), null, asList(new CardExamplePosition(0, 7)), null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "AMAZING"}, outputCompleteReplace,
        asList(true, false, true, false),
        asList(asList(new CardExamplePosition(11, 7)), null, asList(new CardExamplePosition(0, 7)), null)
    );
  }

  @Test
  public void startsWith() throws Exception {
    final File dataFile = temp.newFile("startsWithTest.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"amazing restaurant. Very good food.\" }");
      printWriter.append("{ \"col\" : \"This is a worst restaurant. bad food.\" }");
      printWriter.append("{ \"col\" : \"Amazing environment. Amazing food.\" }");
      printWriter.append("{ \"col\" : \"Not good.\" }");
    }

    ReplacePatternRule rule = new ReplacePatternRule()
        .setSelectionType(ReplaceSelectionType.STARTS_WITH)
        .setSelectionPattern("amazing");
    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'STARTS_WITH', 'amazing', false)", // example
        "CASE WHEN regexp_like(\"input.expr\", '^\\Qamazing\\E.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '^\\Qamazing\\E.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '^\\Qamazing\\E', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '^\\Qamazing\\E.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '^\\Qamazing\\E.*?')" // match expression
    );

    List<Object> outputSelReplace = list(
        (Object)"AMAZING restaurant. Very good food.",
        "This is a worst restaurant. bad food.",
        "Amazing environment. Amazing food.",
        "Not good."
    );

    List<Object> outputCompleteReplace = list(
        (Object)"AMAZING",
        "This is a worst restaurant. bad food.",
        "Amazing environment. Amazing food.",
        "Not good."
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "AMAZING"}, outputSelReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(0, 7)), null, null, null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "AMAZING"}, outputCompleteReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(0, 7)), null, null, null)
    );

    // with ignore case set to false
    rule = rule.setIgnoreCase(true);
    ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'STARTS_WITH', 'amazing', true)", // example
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u)^\\Qamazing\\E.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u)^\\Qamazing\\E.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '(?i)(?u)^\\Qamazing\\E', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u)^\\Qamazing\\E.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '(?i)(?u)^\\Qamazing\\E.*?')" // match expression
    );

    outputSelReplace = list(
        (Object)"AMAZING restaurant. Very good food.",
        "This is a worst restaurant. bad food.",
        "AMAZING environment. Amazing food.",
        "Not good."
    );

    outputCompleteReplace = list(
        (Object)"AMAZING",
        "This is a worst restaurant. bad food.",
        "AMAZING",
        "Not good."
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "AMAZING"}, outputSelReplace,
        asList(true, false, true, false),
        asList(asList(new CardExamplePosition(0, 7)), null, asList(new CardExamplePosition(0, 7)), null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "AMAZING"}, outputCompleteReplace,
        asList(true, false, true, false),
        asList(asList(new CardExamplePosition(0, 7)), null, asList(new CardExamplePosition(0, 7)), null)
    );
  }

  @Test
  public void endsWith() throws Exception {
    final File dataFile = temp.newFile("endsWithTest.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"amazing restaurant. Very good food\" }");
      printWriter.append("{ \"col\" : \"This is a worst restaurant. bad food\" }");
      printWriter.append("{ \"col\" : \"Amazing environment. Amazing Food\" }");
      printWriter.append("{ \"col\" : \"Not good\" }");
    }

    ReplacePatternRule rule = new ReplacePatternRule()
        .setSelectionType(ReplaceSelectionType.ENDS_WITH)
        .setSelectionPattern("food");
    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'ENDS_WITH', 'food', false)", // example
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qfood\\E$') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qfood\\E$') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '\\Qfood\\E$', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '.*?\\Qfood\\E$') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '.*?\\Qfood\\E$')" // match expression
    );

    List<Object> outputSelReplace = list(
        (Object)"amazing restaurant. Very good dinner",
        "This is a worst restaurant. bad dinner",
        "Amazing environment. Amazing Food",
        "Not good"
    );

    List<Object> outputCompleteReplace = list(
        (Object)"dinner",
        "dinner",
        "Amazing environment. Amazing Food",
        "Not good"
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "dinner"}, outputSelReplace,
        asList(true, true, false, false),
        asList(asList(new CardExamplePosition(30, 4)), asList(new CardExamplePosition(32, 4)), null, null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "dinner"}, outputCompleteReplace,
        asList(true, true, false, false),
        asList(asList(new CardExamplePosition(30, 4)), asList(new CardExamplePosition(32, 4)), null, null)
    );

    // with ignore case set to false
    rule = rule.setIgnoreCase(true);
    ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'ENDS_WITH', 'food', true)", // example
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qfood\\E$') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qfood\\E$') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '(?i)(?u)\\Qfood\\E$', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?\\Qfood\\E$') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '(?i)(?u).*?\\Qfood\\E$')" // match expression
    );

    outputSelReplace = list(
        (Object)"amazing restaurant. Very good dinner",
        "This is a worst restaurant. bad dinner",
        "Amazing environment. Amazing dinner",
        "Not good"
    );

    outputCompleteReplace = list(
        (Object)"dinner",
        "dinner",
        "dinner",
        "Not good"
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "dinner"}, outputSelReplace,
        asList(true, true, true, false),
        asList(asList(new CardExamplePosition(30, 4)), asList(new CardExamplePosition(32, 4)), asList(new CardExamplePosition(29, 4)), null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "dinner"}, outputCompleteReplace,
        asList(true, true, true, false),
        asList(asList(new CardExamplePosition(30, 4)), asList(new CardExamplePosition(32, 4)), asList(new CardExamplePosition(29, 4)), null)
    );
  }

  @Test
  public void exact() throws Exception {
    final File dataFile = temp.newFile("exact.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"Los Angeles\" }");
      printWriter.append("{ \"col\" : \"LOS ANGELES\" }");
      printWriter.append("{ \"col\" : \"LOS ANGELES, CA\" }");
      printWriter.append("{ \"col\" : null }");
    }

    ReplacePatternRule rule = new ReplacePatternRule()
        .setSelectionType(ReplaceSelectionType.EXACT)
        .setSelectionPattern("Los Angeles");
    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'EXACT', 'Los Angeles', false)", // example
        "CASE WHEN \"input.expr\" = 'Los Angeles' THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN \"input.expr\" = 'Los Angeles' THEN " + // selection replace
            "'new''''value' ELSE \"input.expr\" END",
        "CASE WHEN \"input.expr\" = 'Los Angeles' THEN 'new value' ELSE \"input.expr\" END", // value replace
        "\"user\".\"col.with.dots\" = 'Los Angeles'" // match expression
    );

    List<Object> outputSelReplace = list(
        (Object)"LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES, CA",
        null
    );

    List<Object> outputCompleteReplace = list(
        (Object)"LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES, CA",
        null
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "LOS ANGELES"}, outputSelReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(0, 11)), null, null, null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "LOS ANGELES"}, outputCompleteReplace,
        asList(true, false, false, false),
        asList(asList(new CardExamplePosition(0, 11)), null, null, null)
    );

    // with ignore case set to false
    rule = rule.setIgnoreCase(true);
    ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'EXACT', 'Los Angeles', true)", // example
        "CASE WHEN lower(\"input.expr\") = lower('Los Angeles') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN lower(\"input.expr\") = lower('Los Angeles') THEN " + // selection replace
            "'new''''value' ELSE \"input.expr\" END",
        "CASE WHEN lower(\"input.expr\") = lower('Los Angeles') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "lower(\"user\".\"col.with.dots\") = lower('Los Angeles')" // match expression
    );

    outputSelReplace = list(
        (Object)"LosAngeles",
        "LosAngeles",
        "LOS ANGELES, CA",
        null
    );

    outputCompleteReplace = list(
        (Object)"LosAngeles",
        "LosAngeles",
        "LOS ANGELES, CA",
        null
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "LosAngeles"}, outputSelReplace,
        asList(true, true, false, false),
        asList(asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 11)), null, null)
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "LosAngeles"}, outputCompleteReplace,
        asList(true, true, false, false),
        asList(asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 11)), null, null)
    );
  }

  @Test
  public void matches() throws Exception {
    final File dataFile = temp.newFile("matches.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"Los Angeles\" }");
      printWriter.append("{ \"col\" : \"LOS Angeles\" }");
      printWriter.append("{ \"col\" : \"LOSANGELES\" }");
      printWriter.append("{ \"col\" : \"LosAngeles, CA\" }");
    }

    ReplacePatternRule rule = new ReplacePatternRule()
        .setSelectionType(ReplaceSelectionType.MATCHES)
        .setSelectionPattern("Los.*Angeles");

    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'MATCHES', 'Los.*Angeles', false)", // example
        "CASE WHEN regexp_like(\"input.expr\", '.*?Los.*Angeles.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '.*?Los.*Angeles.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", 'Los.*Angeles', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '.*?Los.*Angeles.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '.*?Los.*Angeles.*?')" // match expression
    );

    List<Object> outputSelReplace = list(
        (Object)"LOS ANGELES",
        "LOS Angeles",
        "LOSANGELES",
        "LOS ANGELES, CA"
    );

    List<Object> outputCompleteReplace = list(
        (Object)"LOS ANGELES",
        "LOS Angeles",
        "LOSANGELES",
        "LOS ANGELES"
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "LOS ANGELES"}, outputSelReplace,
        asList(true, false, false, true),
        asList(asList(new CardExamplePosition(0, 11)), null, null, asList(new CardExamplePosition(0, 10)))
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "LOS ANGELES"}, outputCompleteReplace,
        asList(true, false, false, true),
        asList(asList(new CardExamplePosition(0, 11)), null, null, asList(new CardExamplePosition(0, 10)))
    );

    // with ignore case set to false
    rule = rule.setIgnoreCase(true);
    ruleWrapper = recommender.wrapRule(rule);

    validateExpressions(
        ruleWrapper,
        "match_pattern_example(\"table.with.dots\".col, 'MATCHES', 'Los.*Angeles', true)", // example
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?Los.*Angeles.*?') THEN NULL ELSE \"input.expr\" END", // null replace
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?Los.*Angeles.*?') THEN " + // selection replace
            "regexp_replace(\"input.expr\", '(?i)(?u)Los.*Angeles', 'new''''value') ELSE \"input.expr\" END",
        "CASE WHEN regexp_like(\"input.expr\", '(?i)(?u).*?Los.*Angeles.*?') THEN 'new value' ELSE \"input.expr\" END", // value replace
        "regexp_like(\"user\".\"col.with.dots\", '(?i)(?u).*?Los.*Angeles.*?')" // match expression
    );

    outputSelReplace = list(
        (Object)"LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES, CA"
    );

    outputCompleteReplace = list(
        (Object)"LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES",
        "LOS ANGELES"
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "LOS ANGELES"}, outputSelReplace,
        asList(true, true, true, true),
        asList(asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 10)), asList(new CardExamplePosition(0, 10)))
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "LOS ANGELES"}, outputCompleteReplace,
        asList(true, true, true, true),
        asList(asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 11)), asList(new CardExamplePosition(0, 10)), asList(new CardExamplePosition(0, 10)))
    );
  }


  @Test
  public void isNull() throws Exception {
    final File dataFile = temp.newFile("nullTest.json");
    try (final PrintWriter printWriter = new PrintWriter(dataFile)) {
      printWriter.append("{ \"col\" : \"Los Angeles\" }");
      printWriter.append("{ \"col\" : null }");
    }

    ReplacePatternRule rule = new ReplacePatternRule()
        .setSelectionType(ReplaceSelectionType.IS_NULL);

    TransformRuleWrapper<ReplacePatternRule> ruleWrapper = recommender.wrapRule(rule);

    assertEquals("CASE WHEN \"input.expr\" IS NULL THEN 'new''''value' ELSE \"input.expr\" END",
        ruleWrapper.getFunctionExpr("\"input.expr\"", ReplaceType.SELECTION, "new''value"));

    assertEquals("CASE WHEN \"input.expr\" IS NULL THEN 'new value' ELSE \"input.expr\" END",
        ruleWrapper.getFunctionExpr("\"input.expr\"", ReplaceType.VALUE, "new value"));

    assertEquals("\"user\".\"col.with.dots\" IS NULL", ruleWrapper.getMatchFunctionExpr("\"user\".\"col.with.dots\""));

    List<Object> output = list(
        (Object)"Los Angeles",
        "Los Angeles"
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.SELECTION, "Los Angeles"}, output,
        asList(false, true),
        null
    );

    validate(dataFile.getAbsolutePath(), ruleWrapper,
        new Object[] { ReplaceType.VALUE, "Los Angeles"}, output,
        asList(false, true),
        null
    );
  }

  /**
   * Helper method to test outputs of given {@link TransformRuleWrapper<ReplacePatternRule>}.
   *
   * @param ruleWrapper
   * @param exampleExpr Example generation expression when the given input expression is: <i>"table.with.dots".col</i>
   * @param nullReplaceExpr expression for replacing the selected expression with null when the input expression is: <i>"input.expr"</i>
   * @param selReplaceExpr expression for replacing selected value with  <i>new''value</i> when the input expression is: <i>"input.expr"</i>
   * @param valueReplaceExpr expression for replacing the whole cell value with <i>new value</i> when the input expression is: <i>"input.expr"</i>
   * @param matchExpr matching expression for given table <i>user</i> and column <i>col.with.dots</i>
   */
  private void validateExpressions(TransformRuleWrapper<ReplacePatternRule> ruleWrapper, String exampleExpr, String nullReplaceExpr,
      String selReplaceExpr, String valueReplaceExpr, String matchExpr) {

    assertEquals(exampleExpr, ruleWrapper.getExampleFunctionExpr("\"table.with.dots\".col"));

    assertEquals(nullReplaceExpr, ruleWrapper.getFunctionExpr("\"input.expr\"", ReplaceType.NULL));

    assertEquals(selReplaceExpr, ruleWrapper.getFunctionExpr("\"input.expr\"", ReplaceType.SELECTION, "new''value"));

    assertEquals(valueReplaceExpr, ruleWrapper.getFunctionExpr("\"input.expr\"", ReplaceType.VALUE, "new value"));

    assertEquals(matchExpr, ruleWrapper.getMatchFunctionExpr("\"user\".\"col.with.dots\""));
  }
}
