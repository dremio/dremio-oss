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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.common.utils.PathUtils;

/**
 * Test compatibility of String and legacy implementation byte[] NamespaceInternalKey.
 */
@RunWith(Parameterized.class)
public class TestNamespaceInternalKeyCompatibility {
  @Parameterized.Parameters
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][]{
      {"a.b.c", true},
      {"a.b.c.d", true},
      {"a", true},
      {"a.b", true},
      {"a1.b.c", true},
      {"a.a.a.a", true},
      {"0", true},
      {"0a.b.3", true},
      {"1.2.3", true},
      {"0.0.0.0", true},
      {"Aa.bB.cC.Dd", true},
      {"0.0.0.0", false},
      {"Aa.bB.cC.Dd", false},
      {"A.A.A.A", false},
      {"1A.2b.3C.4d", false}});
  }

  private String path;
  private boolean normalized;

  public TestNamespaceInternalKeyCompatibility(String path, boolean normalized) {
    this.path = path;
    this.normalized = normalized;
  }

  private NamespaceInternalKey newKey(String path) {
    return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)), normalized);
  }

  private LegacyNamespaceInternalKey newLegacyKey(String path) {
    return new LegacyNamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)), normalized);
  }

  private void verifyRangeEndKey(byte[] expected, byte[] actual) {
    final int expectedKeyLength = expected.length - 1;
    assertEquals(expectedKeyLength, actual.length - NamespaceInternalKey.MAX_2_BYTES_UTF8_VALUE.length);

    final byte[] ex = new byte[expectedKeyLength];
    final byte[] ac = new byte[expectedKeyLength];
    System.arraycopy(expected, 0, ex, 0, expectedKeyLength);
    System.arraycopy(actual, 0, ac, 0, expectedKeyLength);
    assertThat(ac, is(equalTo(ex)));


    final byte[] terminator = NamespaceInternalKey.MAX_2_BYTES_UTF8_VALUE;
    final byte[] lastFourBytes = new byte[4];
    System.arraycopy(actual, actual.length - 4, lastFourBytes, 0, terminator.length);
    assertThat(lastFourBytes, is(equalTo(terminator)));
  }

  @Test
  public void testNamespaceKeyCompatibility() {
    assertThat(newKey(path).getKey().getBytes(StandardCharsets.UTF_8),
      is(equalTo(newLegacyKey(path).getKey())));
  }

  @Test
  public void testNamespaceRangeStartKeyCompatibility() {
    assertThat(newKey(path).getRangeStartKey().getBytes(StandardCharsets.UTF_8),
      is(equalTo(newLegacyKey(path).getRangeStartKey())));
  }

  @Test
  public void testRootLookupStartKey() {
    assertThat(NamespaceInternalKey.getRootLookupStartKey().getBytes(StandardCharsets.UTF_8),
      is(equalTo(LegacyNamespaceInternalKey.getRootLookupStart())));
  }

  @Test
  public void testNamespaceRangeEndKeyCompatibility() {
    final byte[] expected = newLegacyKey(path).getRangeEndKey();
    final byte[] actual = newKey(path).getRangeEndKey().getBytes(StandardCharsets.UTF_8);
    verifyRangeEndKey(expected, actual);
  }

  @Test
  public void testRootLookupEndKey() {
    final byte[] expected = LegacyNamespaceInternalKey.getRootLookupEnd();
    final byte[] actual = NamespaceInternalKey.getRootLookupEndKey().getBytes(StandardCharsets.UTF_8);
    verifyRangeEndKey(expected, actual);
  }

  @Test
  public void testLegacyNamespaceInternalKeyAsString() {
    final String expected = newKey(path).getKey();
    final String actual = new String(newLegacyKey(path).getKey(), StandardCharsets.UTF_8);
    assertEquals(expected, actual);
  }
}
