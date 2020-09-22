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
package com.dremio.exec.planner.sql.handlers.commands;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Predicate;

import org.junit.Test;

import com.dremio.exec.proto.UserProtos.LikeFilter;

/**
 * Tests {@code MetadataProviderConditions}
 */
public class TestMetadataProviderConditions {

  @Test
  public void testTableTypesEmptyList() {
    assertSame(MetadataProviderConditions.ALWAYS_TRUE,
      MetadataProviderConditions.getTableTypePredicate(Collections.emptyList()));
  }

  @Test
  public void testTableTypes() {
    Predicate<String> filter =
      MetadataProviderConditions.getTableTypePredicate(Arrays.asList("foo", "bar"));

    assertTrue(filter.test("foo"));
    assertTrue(filter.test("bar"));
    assertFalse(filter.test("baz"));
    assertFalse(filter.test("fooo"));
    assertFalse(filter.test("ofoo"));
    assertFalse(filter.test("FOO"));
  }

  @Test
  public void testLikeFilterAlwaysTrue() {
    assertSame(MetadataProviderConditions.ALWAYS_TRUE,
      MetadataProviderConditions.getCatalogNamePredicate(null));
    assertSame(MetadataProviderConditions.ALWAYS_TRUE,
      MetadataProviderConditions.getCatalogNamePredicate(newLikeFilter(null, "\\")));
    assertSame(MetadataProviderConditions.ALWAYS_TRUE,
      MetadataProviderConditions.getCatalogNamePredicate(newLikeFilter("%", "\\")));
  }

  @Test
  public void testLikeFilter() {
    Predicate<String> filter = MetadataProviderConditions.getCatalogNamePredicate(newLikeFilter("abc", "\\"));
    assertTrue(filter.test("abc"));
    assertFalse(filter.test("abcd"));
    assertTrue(filter.test("ABC"));
  }

  @Test
  public void testLikeFilterMixedCase() {
    Predicate<String> filter = MetadataProviderConditions.getCatalogNamePredicate(newLikeFilter("AbC", "\\"));
    assertTrue(filter.test("abc"));
    assertFalse(filter.test("abcd"));
    assertFalse(filter.test("aabc"));
    assertTrue(filter.test("ABC"));
  }

  @Test
  public void testCreateFilterAlwaysTrue() {
    assertFalse(MetadataProviderConditions.createConjunctiveQuery(null, null).isPresent());
    assertFalse(MetadataProviderConditions.createConjunctiveQuery(null, newLikeFilter("%", null)).isPresent());
    assertFalse(MetadataProviderConditions.createConjunctiveQuery(newLikeFilter("%", null), null).isPresent());
    assertFalse(MetadataProviderConditions.createConjunctiveQuery(newLikeFilter("%", null), newLikeFilter("%", null))
      .isPresent());
  }

  private static LikeFilter newLikeFilter(String pattern, String escape) {
    LikeFilter.Builder builder = LikeFilter.newBuilder();
    if (pattern != null) {
      builder.setPattern(pattern);
    }
    if (escape != null) {
      builder.setEscape(escape);
    }
    return builder.build();
  }
}
