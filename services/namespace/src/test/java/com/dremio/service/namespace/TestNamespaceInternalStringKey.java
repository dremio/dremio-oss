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
import static com.dremio.service.namespace.NamespaceInternalKeyDumpUtil.extractRangeKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests NamespaceInternalKey. */
@RunWith(Enclosed.class)
public class TestNamespaceInternalStringKey {

  /** Test key validity with UTF-8 2-bytes value restraints. */
  public static class TestInvalidKey {
    @Test(expected = UserException.class)
    public void testSmallestInvalidKey() {
      new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(generatePath(128))));
    }

    @Test
    public void testLargestValidKey() {
      new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(generatePath(127))));
    }

    private String generatePath(int depth) {
      final String delimiter = ".";
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < depth; i++) {
        if (i > 0) {
          builder.append(delimiter);
        }
        builder.append(i);
      }
      return builder.toString();
    }
  }

  /** Test NamespaceInternalKey String keys. */
  @RunWith(Parameterized.class)
  public static class TestStringKeys {
    @Parameterized.Parameters
    public static Collection<Object[]> input() {
      return Arrays.asList(
          new Object[][] {
            // inputPath, expectedKey, expectedRangeStartKey, expectedRangeEndKey
            {"a.b.c", "2.a.1.b.0.c", "3.a.2.b.1.c.0.", "3.a.2.b.1.c.0."},
            {"a.b.c.d", "3.a.2.b.1.c.0.d", "4.a.3.b.2.c.1.d.0.", "4.a.3.b.2.c.1.d.0."},
            {"a", "0.a", "1.a.0.", "1.a.0."},
            {"a.b", "1.a.0.b", "2.a.1.b.0.", "2.a.1.b.0."},
            {"a1.b.c", "2.a1.1.b.0.c", "3.a1.2.b.1.c.0.", "3.a1.2.b.1.c.0."},
            {"a.a.a.a", "3.a.2.a.1.a.0.a", "4.a.3.a.2.a.1.a.0.", "4.a.3.a.2.a.1.a.0."},
            {"0", "0.0", "1.0.0.", "1.0.0."},
            {"0a.b.3", "2.0a.1.b.0.3", "3.0a.2.b.1.3.0.", "3.0a.2.b.1.3.0."},
            {"1.2.3", "2.1.1.2.0.3", "3.1.2.2.1.3.0.", "3.1.2.2.1.3.0."},
            {"0.0.0.0", "3.0.2.0.1.0.0.0", "4.0.3.0.2.0.1.0.0.", "4.0.3.0.2.0.1.0.0."},
            {
              "Aa.bB.cC.Dd",
              "3.aa.2.bb.1.cc.0.dd",
              "4.aa.3.bb.2.cc.1.dd.0.",
              "4.aa.3.bb.2.cc.1.dd.0."
            },
            {
              "1A.2b.3C.4d",
              "3.1a.2.2b.1.3c.0.4d",
              "4.1a.3.2b.2.3c.1.4d.0.",
              "4.1a.3.2b.2.3c.1.4d.0."
            }
          });
    }

    private final String inputPath;
    private final String expectedKey;
    private final String expectedRangeStartKey;
    private final String expectedRangeEndKey;

    public TestStringKeys(
        String inputPath,
        String expectedKey,
        String expectedRangeStartKey,
        String expectedRangeEndKey) {
      this.inputPath = inputPath;
      this.expectedKey = expectedKey;
      this.expectedRangeStartKey = expectedRangeStartKey;
      this.expectedRangeEndKey = expectedRangeEndKey;
    }

    private NamespaceInternalKey newKey(String path) {
      return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)));
    }

    private NamespaceInternalKey parseKey(byte[] keyBytes) {
      String path = extractKey(keyBytes, false);
      return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)));
    }

    @Test
    public void testNamespaceInternalKeys() {
      assertEquals(
          expectedKey,
          extractKey(newKey(inputPath).getKey().getBytes(StandardCharsets.UTF_8), true));
    }

    @Test
    public void testNamespaceInternalRangeStartKeys() {
      assertEquals(
          expectedRangeStartKey,
          extractRangeKey(newKey(inputPath).getRangeStartKey().getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testNamespaceInternalRangeEndKeys() {
      assertEquals(
          expectedRangeEndKey,
          extractRangeKey(newKey(inputPath).getRangeEndKey().getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testParsedKeyPath() {
      final NamespaceInternalKey key = newKey(inputPath);
      final NamespaceInternalKey parsedKey =
          parseKey(key.getKey().getBytes(StandardCharsets.UTF_8));
      final NamespaceKey expectedPath = key.getPath().asLowerCase();
      assertThat(parsedKey.getPath()).isEqualTo(expectedPath);
    }

    @Test
    public void testParsedKey() {
      final NamespaceInternalKey key = newKey(inputPath);
      final NamespaceInternalKey parsedKey =
          parseKey(key.getKey().getBytes(StandardCharsets.UTF_8));
      assertThat(parsedKey.getKey()).isEqualTo(key.getKey());
    }

    @Test
    public void testParsedRangeStartKey() {
      final NamespaceInternalKey key = newKey(inputPath);
      final NamespaceInternalKey parsedKey =
          parseKey(key.getKey().getBytes(StandardCharsets.UTF_8));
      assertThat(parsedKey.getRangeStartKey()).isEqualTo(key.getRangeStartKey());
    }

    @Test
    public void testParsedRangeEndKey() {
      final NamespaceInternalKey key = newKey(inputPath);
      final NamespaceInternalKey parsedKey =
          parseKey(key.getKey().getBytes(StandardCharsets.UTF_8));
      assertThat(parsedKey.getRangeEndKey()).isEqualTo(key.getRangeEndKey());
    }

    @Test
    public void testPathProcessing() {
      final String paths = inputPath.toLowerCase();
      final List<String> expectedPaths =
          Arrays.asList(paths.split("[" + NamespaceInternalKey.PATH_DELIMITER + "]"));
      final List<String> actualPaths =
          NamespaceInternalKey.processPathComponents(
              new NamespaceKey(PathUtils.parseFullPath(inputPath)));
      assertThat(actualPaths).isEqualTo(expectedPaths);
    }
  }
}
