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
package com.dremio.exec.planner.sql.handlers.commands;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.google.common.base.Predicate;

/**
 * Tests {@code MetadataProviderConditions}
 */
public class TestMetadataProviderConditions {

  @Test
  public void testTableTypesEmptyList() {
    assertSame(MetadataProviderConditions.ALWAYS_TRUE, MetadataProviderConditions.getTableTypePredicate(Collections.emptyList()));
  }

  @Test
  public void testTableTypes() {
    Predicate<String> filter = MetadataProviderConditions.getTableTypePredicate(Arrays.asList("foo", "bar"));

    assertTrue(filter.apply("foo"));
    assertTrue(filter.apply("bar"));
    assertFalse(filter.apply("baz"));
    assertFalse(filter.apply("fooo"));
    assertFalse(filter.apply("ofoo"));
    assertFalse(filter.apply("FOO"));
  }

  @Test
  public void testLikeFilterAlwaysTrue() {
    assertSame(MetadataProviderConditions.ALWAYS_TRUE, MetadataProviderConditions.getLikePredicate(null));
    assertSame(MetadataProviderConditions.ALWAYS_TRUE, MetadataProviderConditions.getLikePredicate(newLikeFilter(null, "\\")));
    assertSame(MetadataProviderConditions.ALWAYS_TRUE, MetadataProviderConditions.getLikePredicate(newLikeFilter("%", "\\")));
  }

  @Test
  public void testLikeFilter() {
    Predicate<String> filter = MetadataProviderConditions.getLikePredicate(newLikeFilter("abc", "\\"));
    assertTrue(filter.apply("abc"));
    assertFalse(filter.apply("abcd"));
    assertTrue(filter.apply("ABC"));
  }

  @Test
  public void testLikeFilterMixedCase() {
    Predicate<String> filter = MetadataProviderConditions.getLikePredicate(newLikeFilter("AbC", "\\"));
    assertTrue(filter.apply("abc"));
    assertFalse(filter.apply("abcd"));
    assertFalse(filter.apply("aabc"));
    assertTrue(filter.apply("ABC"));
  }

  @Test
  public void testCreateFilterAlwaysTrue() {
    assertNull(MetadataProviderConditions.createFilter(null, null));
    assertNull(MetadataProviderConditions.createFilter(null, newLikeFilter("%", null)));
    assertNull(MetadataProviderConditions.createFilter(newLikeFilter("%", null), null));
    assertNull(MetadataProviderConditions.createFilter(newLikeFilter("%", null), newLikeFilter("%", null)));
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
