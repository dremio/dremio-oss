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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;

/**
 * Tests for {@link CardGenerator}
 */
public class TestCardGenerator {
  private CardGenerator cardGenerator = new CardGenerator(null, null, null);
  private ReplaceRecommender replaceRecommender = new ReplaceRecommender();
  private List<TransformRuleWrapper<ReplacePatternRule>> rules = asList(
      replaceRecommender.wrapRule(
          new ReplacePatternRule(ReplaceSelectionType.CONTAINS)
              .setSelectionPattern("test pattern")
              .setIgnoreCase(false)
      ),
      replaceRecommender.wrapRule(
          new ReplacePatternRule(ReplaceSelectionType.MATCHES)
              .setSelectionPattern(".*txt.*")
              .setIgnoreCase(true)
      )
  );

  /**
   * Tests {@link CardGenerator#generateCardGenQuery(String, String, List)}
   */
  @Test
  public void exampleGeneratorQuery() {
    // Expect backticks around column name as it has special characters in it.
    String outputQuery1 = cardGenerator.generateCardGenQuery("col.with.dots",
        "TABLE(\"jobstore\".\"path\"(type => 'arrow))", rules);
    String expQuery1 = "SELECT\n" +
        "match_pattern_example(dremio_preview_data.\"col.with.dots\", 'CONTAINS', 'test pattern', false) AS example_0,\n" +
        "match_pattern_example(dremio_preview_data.\"col.with.dots\", 'MATCHES', '.*txt.*', true) AS example_1,\n" +
        "dremio_preview_data.\"col.with.dots\" AS inputCol\n" +
        "FROM TABLE(\"jobstore\".\"path\"(type => 'arrow)) as dremio_preview_data\n" +
        "WHERE \"col.with.dots\" IS NOT NULL\n" +
        "LIMIT 3";

    assertEquals(expQuery1, outputQuery1);

    // Column name expecting no backticks
    String outputQuery2 = cardGenerator.generateCardGenQuery("normalCol",
        "TABLE(\"jobstore\".\"path\"(type => 'arrow))", rules);
    String expQuery2 = "SELECT\n" +
        "match_pattern_example(dremio_preview_data.normalCol, 'CONTAINS', 'test pattern', false) AS example_0,\n" +
        "match_pattern_example(dremio_preview_data.normalCol, 'MATCHES', '.*txt.*', true) AS example_1,\n" +
        "dremio_preview_data.normalCol AS inputCol\n" +
        "FROM TABLE(\"jobstore\".\"path\"(type => 'arrow)) as dremio_preview_data\n" +
        "WHERE normalCol IS NOT NULL\n" +
        "LIMIT 3";

    assertEquals(expQuery2, outputQuery2);
  }

  /**
   * Tests for {@link CardGenerator#generateMatchCountQuery(String, String, List)}
   */
  @Test
  public void matchCountGeneratorQuery() {
    // Expect backticks around column name as it has special characters in it.
    String outputQuery1 = cardGenerator.generateMatchCountQuery("col with spaces",
        "TABLE(\"jobstore\".\"path\"(type => 'arrow))", rules);
    String expQuery1 = "SELECT\n" +
        "sum(CASE WHEN regexp_like(dremio_preview_data.\"col with spaces\", '.*?\\Qtest pattern\\E.*?') THEN 1 ELSE 0 END) AS matched_count_0,\n" +
        "sum(CASE WHEN regexp_like(dremio_preview_data.\"col with spaces\", '(?i)(?u).*?.*txt.*.*?') THEN 1 ELSE 0 END) AS matched_count_1,\n" +
        "COUNT(1) as total\n" +
        "FROM TABLE(\"jobstore\".\"path\"(type => 'arrow)) as dremio_preview_data";

    assertEquals(expQuery1, outputQuery1);

    // Column name expecting no backticks
    String outputQuery2 = cardGenerator.generateMatchCountQuery("normalCol",
        "TABLE(\"jobstore\".\"path\"(type => 'arrow))", rules);
    String expQuery2 = "SELECT\n" +
        "sum(CASE WHEN regexp_like(dremio_preview_data.normalCol, '.*?\\Qtest pattern\\E.*?') THEN 1 ELSE 0 END) AS matched_count_0,\n" +
        "sum(CASE WHEN regexp_like(dremio_preview_data.normalCol, '(?i)(?u).*?.*txt.*.*?') THEN 1 ELSE 0 END) AS matched_count_1,\n" +
        "COUNT(1) as total\n" +
        "FROM TABLE(\"jobstore\".\"path\"(type => 'arrow)) as dremio_preview_data";

    assertEquals(expQuery2, outputQuery2);
  }
}
