/*
 * Copyright (C) 2017 Dremio Corporation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.dremio.common.utils.PathUtils;

/**
 * Tests for internal key used by namespace.
 */
public class TestNamespaceInternalKey {
  @Test
  public void testNamespaceIntToPrefix() throws Exception {
    for (int n = 0; n < 1 << 16; ++n) {
      assertEquals(n, NamespaceInternalKey.prefixBytesToInt(NamespaceInternalKey.toPrefixBytes(n)));
    }
  }

  @Test
  public void testNamespaceKey() throws Exception {
    assertEquals("2.a.1.b.0.c", NamespaceInternalKey.extractKey(newKey("a.b.c").getKey(), true));
    assertEquals("2.a1.1.b.0.c", NamespaceInternalKey.extractKey(newKey("a1.b.c").getKey(), true));
    assertEquals("0.a", NamespaceInternalKey.extractKey(newKey("a").getKey(), true));
    assertEquals("1.a.0.b", NamespaceInternalKey.extractKey(newKey("a.b").getKey(), true));
    assertEquals("3.a.2.a.1.a.0.a", NamespaceInternalKey.extractKey(newKey("a.a.a.a").getKey(), true));
  }

  @Test
  public void testNamespaceRangeKey() throws Exception {
    assertEquals("3.a.2.b.1.c.0.", NamespaceInternalKey.extractRangeKey(newKey("a.b.c").getRangeStartKey()));
    assertEquals("3.a1.2.b.1.c.0.", NamespaceInternalKey.extractRangeKey(newKey("a1.b.c").getRangeStartKey()));
    assertEquals("1.a.0.", NamespaceInternalKey.extractRangeKey(newKey("a").getRangeStartKey()));
    assertEquals("2.a.1.b.0.", NamespaceInternalKey.extractRangeKey(newKey("a.b").getRangeStartKey()));
    assertEquals("4.a.3.a.2.a.1.a.0.", NamespaceInternalKey.extractRangeKey(newKey("a.a.a.a").getRangeStartKey()));
    assertEquals("4.a.3.a.2.a.1.a.0.", NamespaceInternalKey.extractRangeKey(newKey("a.a.a.a").getRangeEndKey()));
  }

  @Test
  public void testNamespaceParseKey() throws Exception {
    NamespaceInternalKey key = newKey("a.b.c");
    NamespaceInternalKey parsedKey = NamespaceInternalKey.parseKey(key.getKey());
    assertEquals(key.getPath(), parsedKey.getPath());
    assertTrue(Arrays.equals(key.getKey(), parsedKey.getKey()));

    key = newKey("a.b.c");
    parsedKey = NamespaceInternalKey.parseKey(key.getKey());
    assertEquals(key.getPath(), parsedKey.getPath());

    key = newKey("a");
    parsedKey = NamespaceInternalKey.parseKey(key.getKey());
    assertEquals(key.getPath(), parsedKey.getPath());

    key = newKey("a.b");
    parsedKey = NamespaceInternalKey.parseKey(key.getKey());
    assertEquals(key.getPath(), parsedKey.getPath());

    key = newKey("a.a.a.a");
    parsedKey = NamespaceInternalKey.parseKey(key.getKey());
    assertEquals(key.getPath(), parsedKey.getPath());
  }

  private static NamespaceInternalKey newKey(String path) {
    return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)));
  }
}
