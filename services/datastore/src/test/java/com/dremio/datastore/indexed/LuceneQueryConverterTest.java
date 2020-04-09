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
package com.dremio.datastore.indexed;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link LuceneQueryConverter}
 */
public class LuceneQueryConverterTest {
  private static final String TEST_FIELD = "field";

  private static final ImmutableList<String> TEST_VALUES = new ImmutableList.Builder<String>()
    .add("test * test")
    .add("test ? test")
    .add("test \\ test")
    .add("test *\\ test")
    .add("test ?\\ test")
    .add("test ?* test")
    .add("test test*")
    .build();

  private static final ImmutableList<String> CONTAINS_EXPECTED_RESULTS = new ImmutableList.Builder<String>()
    .add("field:*test \\* test*")
    .add("field:*test \\? test*")
    .add("field:*test \\\\ test*")
    .add("field:*test \\*\\\\ test*")
    .add("field:*test \\?\\\\ test*")
    .add("field:*test \\?\\* test*")
    .add("field:*test test\\**")
    .build();

  private static final ImmutableList<String> PREFIX_EXPECTED_RESULTS = new ImmutableList.Builder<String>()
    .add("field:test * test*")
    .add("field:test ? test*")
    .add("field:test \\ test*")
    .add("field:test *\\ test*")
    .add("field:test ?\\ test*")
    .add("field:test ?* test*")
    .add("field:test test**")
    .build();

  private void test(String expected, SearchQuery searchQuery) {
    final LuceneQueryConverter converter = new LuceneQueryConverter();
    assertEquals(expected, converter.toLuceneQuery(searchQuery).toString());
  }

  @Test
  public void testContainsQuery() {
    Preconditions.checkArgument(CONTAINS_EXPECTED_RESULTS.size() == TEST_VALUES.size(),
      "Number of test values do not match the number of expected results provided.");
    for (int i=0; i < TEST_VALUES.size(); i++) {
      test(CONTAINS_EXPECTED_RESULTS.get(i), SearchQueryUtils.newContainsTerm(TEST_FIELD, TEST_VALUES.get(i)));
    }
  }

  @Test
  public void testPrefixQuery() {
    Preconditions.checkArgument(PREFIX_EXPECTED_RESULTS.size() == TEST_VALUES.size(),
      "Number of test values do not match the number of expected results provided.");
    for (int i=0; i < TEST_VALUES.size(); i++) {
      test(PREFIX_EXPECTED_RESULTS.get(i), SearchQueryUtils.newPrefixQuery(TEST_FIELD, TEST_VALUES.get(i)));
    }
  }
}
