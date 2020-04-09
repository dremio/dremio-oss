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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.NamespaceInternalKeyDumpUtil.extractKey;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.common.utils.PathUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;

/**
 * Tests sort order of NamespaceInternalKey.
 *    - Sort order of String keys.
 *    - Sort order of keys encoded in legacy format and current format for backwards compatibility.
 */
@RunWith(Parameterized.class)
public class TestNamespaceInternalKeySortOrder {
  private static List<String> PATHS = ImmutableList.of(
    "a.foo.ds2",
    "0",
    "c.foo.ds1",
    "a.foo.ds1",
    "a.foo.bar1.bar3.ds6",
    "a.foo.bar2.ds4",
    "c.bar",
    "a.ds0",
    "a.foo.bar1.ds3",
    "a.foo.bar1.bar3.ds5",
    "a"
  );

  private static List<String> NUMBERED_PATHS = ImmutableList.of(
    "a.b.c",
    "a0",
    "0.1.23.50",
    "0.2",
    "0.1",
    "0.1.22.33.4",
    "0.1.3",
    "0.1.3.4.5",
    "0.1.23.34",
    "0.1.22.33",
    "0.1.2",
    "0"
  );

  private static List<String> MIXED_CASING_PATHS = ImmutableList.of(
    "0A",
    "A.FOo.dS2",
    "0a",
    "c.Foo.ds1",
    "A",
    "A.foo.ds1",
    "a.foo.bar2.ds4",
    "C.bAr",
    "a.DS0",
    "A.Foo.Bar1.bar3.ds6",
    "a.foo.bar1.ds3",
    "A.Foo.BAR1.bar3.DS5",
    "a"
  );


  @Parameterized.Parameters
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][]{
      {PATHS, true},
      {NUMBERED_PATHS, true},
      {MIXED_CASING_PATHS, true},
      {PATHS, false},
      {NUMBERED_PATHS, false},
      {MIXED_CASING_PATHS, false}
    });
  }

  private final List<String> input;
  private final boolean normalize;
  private final List<Map.Entry<String, NamespaceInternalKey>> expected;

  public TestNamespaceInternalKeySortOrder(List<String> input, boolean normalize) {
    this.input = input;
    this.normalize = normalize;
    this.expected = generateExpectedResults();
  }

  @Test
  public void testSortOrderStringKeys() {
    final List<NamespaceInternalKey> actual = new ArrayList<>();
    input.forEach(path -> actual.add(newKey(path)));
    actual.sort(new NamespaceInternalKeyComparator());
    assertSortOrderEqual(actual);
  }

  @Test
  public void testSortOrderBackwardsCompatibilityAsBytes() {
    final List<byte[]> actual = new ArrayList<>();
    int count = 0;
    for (int i = 0; i < input.size(); i++) {
      final String path = input.get(i);
      actual.add((count % 2 == 0)? newLegacyKey(path).getKey() :
        newKey(path).getKey().getBytes(StandardCharsets.UTF_8));
      count++;
    }
    actual.sort(UnsignedBytes.lexicographicalComparator());
    assertSortOrderEqualBytes(actual);
  }

  @Test
  public void testSortOrderLegacyKeysAsString() {
    final List<String> actual = new ArrayList<>();
    input.forEach(path -> actual.add(new String(newLegacyKey(path).getKey(), StandardCharsets.UTF_8)));
    actual.sort(Comparator.naturalOrder());
    assertSortOrderEqualStrings(actual);
  }

  @Test
  public void testSortOrderBackwardsCompatibilityAsString() {
    final List<String> actual = new ArrayList<>();
    int count = 0;
    for (int i = 0; i < input.size(); i++) {
      final String path = input.get(i);
      actual.add((count % 2 == 0)? new String(newLegacyKey(path).getKey(), StandardCharsets.UTF_8) :
        newKey(path).getKey());
      count++;
    }
    actual.sort(Comparator.naturalOrder());
    assertSortOrderEqualStrings(actual);
  }

  /**
   * Comparator for sorting expected results.
   */
  private class KeyMapComparator implements Comparator<Map.Entry<String, NamespaceInternalKey>> {
    @Override
    public int compare(Map.Entry<String, NamespaceInternalKey> o1, Map.Entry<String, NamespaceInternalKey> o2) {
      return Comparator.<String>naturalOrder().compare(o1.getKey(), o2.getKey());
    }
  }

  /**
   * Comparator for sorting actual results.
   */
  private class NamespaceInternalKeyComparator implements Comparator<NamespaceInternalKey> {
    @Override
    public int compare(NamespaceInternalKey o1, NamespaceInternalKey o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  /**
   * Generates a list of map entries representing the expected result.
   *     - The key of each entry is the extracted String key with prefixes encoded.
   *     - The value of each entry is the NamespaceInternalKey corresponding to each extracted key String.
   * The expected results are sorted by its extracted String key with prefixes levels encoded.
   *
   * @return a list of entries as the expected result.
   */
  private List<Map.Entry<String, NamespaceInternalKey>> generateExpectedResults() {
    final ArrayList<Map.Entry<String, NamespaceInternalKey>> expected = new ArrayList<>();
    input.forEach(path -> {
      final NamespaceInternalKey key = newKey(path);
      final String keyWithPrefix = extractKey(key.getKey().getBytes(StandardCharsets.UTF_8), true);
      expected.add(new AbstractMap.SimpleEntry<>(keyWithPrefix, key));
    });
    expected.sort(new KeyMapComparator());
    return expected;
  }

  /**
   * Creates a new NamespaceInternalKey.
   *
   * @param path paths to create the key from.
   * @return NamespaceInternalKey.
   */
  private NamespaceInternalKey newKey(String path) {
    return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)), normalize);
  }

  /**
   * Creates a new LegacyNamespaceInternalKey.
   *
   * @param path paths to create the key from.
   * @return LegacyNamespaceInternalKey.
   */
  private LegacyNamespaceInternalKey newLegacyKey(String path) {
    return new LegacyNamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)), normalize);
  }

  /**
   * Checks that the sort order of the actual result is the same as the expected result.
   *
   * @param actual the actual result.
   */
  private void assertSortOrderEqual(List<NamespaceInternalKey> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getValue(), actual.get(i));
    }
  }

  /**
   * Checks that the sort order of the actual result is the same as the expected result.
   *
   * @param actual the actual result.
   */
  private void assertSortOrderEqualStrings(List<String> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getValue().getKey(), actual.get(i));
    }
  }

  /**
   * Checks that the sort order of the actual result is the same as the expected result.
   *
   * @param actual the actual result.
   */
  private void assertSortOrderEqualBytes(List<byte[]> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.get(i),
        is(equalTo(expected.get(i).getValue().getKey().getBytes(StandardCharsets.UTF_8))));
    }
  }
}
