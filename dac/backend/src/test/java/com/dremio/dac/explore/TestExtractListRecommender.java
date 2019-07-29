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

import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_END;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_START;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractListRule;
import com.dremio.dac.proto.model.dataset.ExtractListRuleType;
import com.dremio.dac.proto.model.dataset.ExtractRuleMultiple;
import com.dremio.dac.proto.model.dataset.ExtractRuleSingle;
import com.dremio.dac.proto.model.dataset.ListSelection;
import com.dremio.dac.proto.model.dataset.Offset;

/**
 * Tests for {@link ExtractListRecommender}
 */
public class TestExtractListRecommender extends RecommenderTestBase {

  private ExtractListRecommender recommender = new ExtractListRecommender();
  private static String dataFile;

  @BeforeClass
  public static void genData() throws Exception {
    File file = temp.newFile("extractList.json");
    dataFile = file.getAbsolutePath();
    try(PrintWriter writer = new PrintWriter(file)) {
      writer.write(
          "{ \"col\" : [ \"aa\", \"bbb\", \"cccc\" ] }\n" +
          "{ \"col\" : [ \"ddd\", \"e\"] }\n" +
          "{ \"col\" : [ \"fffff\"] }\n" +
          "{ \"col\" : null }"
      );
    }
  }

  @Test
  public void ruleSuggestionsSingleElement() throws Exception {
    List<ExtractListRule> rules = recommender.getRules(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 3, 3), DataType.LIST);
    assertEquals(1, rules.size());
    assertEquals(ExtractListRuleType.single, rules.get(0).getType());
    assertEquals(0, rules.get(0).getSingle().getIndex().intValue());
  }

  @Test
  public void ruleSuggestionsMultiElement() throws Exception {
    List<ExtractListRule> rules = recommender.getRules(new Selection("foo", "[ \"foo\", \"bar\", \"baz\" ]", 3, 10), DataType.LIST);
    assertEquals(4, rules.size());
    compare(new Offset(0, FROM_THE_START), new Offset(1, FROM_THE_START), rules.get(0));
    compare(new Offset(0, FROM_THE_START), new Offset(1, FROM_THE_END), rules.get(1));
    compare(new Offset(2, FROM_THE_END), new Offset(1, FROM_THE_START), rules.get(2));
    compare(new Offset(2, FROM_THE_END), new Offset(1, FROM_THE_END), rules.get(3));
  }

  private static void compare(Offset start, Offset end, ExtractListRule rule) {
    assertEquals(ExtractListRuleType.multiple, rule.getType());
    assertEquals(start, rule.getMultiple().getSelection().getStart());
    assertEquals(end, rule.getMultiple().getSelection().getEnd());
  }

  @Test
  public void testExtractListCardGen() throws Exception {
    {
      ExtractListRule rule = new ExtractRuleSingle(0).wrap();
      TransformRuleWrapper<ExtractListRule> wrapper = recommender.wrapRule(rule);

      assertEquals("\"tbl name\".foo[0] IS NOT NULL", wrapper.getMatchFunctionExpr("\"tbl name\".foo"));
      assertEquals("\"tbl name\".foo[0]", wrapper.getFunctionExpr("\"tbl name\".foo"));
      assertEquals("Element: 0", wrapper.describe());
      validate(dataFile, wrapper,
          new Object[0], list((Object)"aa", "ddd", "fffff", null),
          list(true, true, true, false),
          null
      );
    }
    {
      ExtractListRule rule = new ExtractRuleMultiple(new ListSelection(new Offset(1, FROM_THE_START), new Offset(2, FROM_THE_START))).wrap();
      TransformRuleWrapper<ExtractListRule> wrapper = recommender.wrapRule(rule);

      assertEquals("array_length(sublist(tbl.foo, 2, 2)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("sublist(tbl.foo, 2, 2)", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 2", wrapper.describe());
      validate(dataFile, wrapper,
          new Object[0], list((Object)list("bbb", "cccc"), list("e"), null, null),
          list(true, true, false, false),
          null
      );
    }
    {
      ExtractListRule rule = new ExtractRuleMultiple(new ListSelection(new Offset(1, FROM_THE_END), new Offset(0, FROM_THE_END))).wrap();
      TransformRuleWrapper<ExtractListRule> wrapper = recommender.wrapRule(rule);

      assertEquals("array_length(sublist(tbl.foo, -2, 2)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("sublist(tbl.foo, -2, 2)", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 0 (both from the end)", wrapper.describe());
      validate(dataFile, wrapper,
          new Object[0], list((Object)list("bbb", "cccc"), list("ddd", "e"), null, null),
          list(true, true, false, false),
          null
      );
    }
    {
      ExtractListRule rule = new ExtractRuleMultiple(new ListSelection(new Offset(1, FROM_THE_START), new Offset(1, FROM_THE_END))).wrap();
      TransformRuleWrapper<ExtractListRule> wrapper = recommender.wrapRule(rule);

      assertEquals("array_length(sublist(tbl.foo, 2, array_length(tbl.foo) - 2)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("sublist(tbl.foo, 2, array_length(tbl.foo) - 2)", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("Elements: 1 - 1 (from the end)", wrapper.describe());
      validate(dataFile, wrapper,
          new Object[0], list((Object)list("bbb"), null, null, null),
          list(true, false, false, false),
          null
      );
    }
    {
      ExtractListRule rule = new ExtractRuleMultiple(new ListSelection(new Offset(2, FROM_THE_END), new Offset(2, FROM_THE_START))).wrap();
      TransformRuleWrapper<ExtractListRule> wrapper = recommender.wrapRule(rule);

      assertEquals("array_length(sublist(tbl.foo, -3, -array_length(tbl.foo) + 6)) > 0", wrapper.getMatchFunctionExpr("tbl.foo"));
      assertEquals("sublist(tbl.foo, -3, -array_length(tbl.foo) + 6)", wrapper.getFunctionExpr("tbl.foo"));
      assertEquals("Elements: 2 (from the end) - 2", wrapper.describe());
      validate(dataFile, wrapper,
          new Object[0], list((Object)list("aa", "bbb", "cccc"), null, null, null),
          list(true, false, false, false),
          null
      );
    }
  }
}
