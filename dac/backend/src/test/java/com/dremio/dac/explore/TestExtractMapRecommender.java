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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.dremio.dac.explore.Recommender.TransformRuleWrapper;
import com.dremio.dac.explore.model.extract.MapSelection;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExtractMapRule;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link ExtractMapRecommender}
 */
public class TestExtractMapRecommender {
  private ExtractMapRecommender recommender = new ExtractMapRecommender();

  @Test
  public void testExtractMapRules() throws Exception {
    List<ExtractMapRule> rules = recommender.getRules(new MapSelection("foo", ImmutableList.of("a")), DataType.MAP);
    assertEquals(1, rules.size());
    assertEquals("a", rules.get(0).getPath());
  }

  @Test
  public void testGenExtractMapRuleWrapper() throws Exception {
    TransformRuleWrapper<ExtractMapRule> wrapper = recommender.wrapRule(new ExtractMapRule("a"));
    assertEquals("a", wrapper.getRule().getPath());
    assertEquals("extract from map a", wrapper.describe());
    assertEquals("tbl.foo.a IS NOT NULL", wrapper.getMatchFunctionExpr("tbl.foo"));
    assertEquals("tbl.foo.a", wrapper.getFunctionExpr("tbl.foo"));

    TransformRuleWrapper<ExtractMapRule> wrapper2 = recommender.wrapRule(new ExtractMapRule("c[0].b"));
    assertEquals("extract from map c[0].b", wrapper2.describe());
    assertEquals("tbl.foo.c[0].b IS NOT NULL", wrapper2.getMatchFunctionExpr("tbl.foo"));
    assertEquals("tbl.foo.c[0].b", wrapper2.getFunctionExpr("tbl.foo"));

    // Not adding any validation of the function or match expression as they are just map/list element references
    // which are covered in core execution engine/Arrow tests.
  }
}
