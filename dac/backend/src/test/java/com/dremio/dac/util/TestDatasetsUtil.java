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
package com.dremio.dac.util;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class TestDatasetsUtil {

  @RunWith(Parameterized.class)
  public static class TemporaryPath {
    private final List<String> value;
    private final boolean expected;

    public TemporaryPath(final List<String> value, final boolean expected) {
      this.value = value;
      this.expected = expected;
    }

    @Parameterized.Parameters(name = "dataset path {0}, expected {1})")
    public static Collection<Object[]> data() {
      return asList(
          new Object[][] {
            {null, false},
            {emptyList(), false},
            {singletonList(null), false},
            {singletonList(""), false},
            {asList(null, null), false},
            {asList("", ""), false},
            {asList(null, ""), false},
            {asList("", null), false},
            {singletonList("tmp"), false},
            {asList("tmp", "TEST"), false},
            {asList("test", ""), false},
            {asList("test", "test"), false},
            {asList("test", "UNTITLED"), false},
            {asList("test", "tmp", "UNTITLED"), false},
            {asList("tmp", "test", "UNTITLED"), false},
            {asList("tmp", "UNTITLED"), true},
            {asList("tmp/hello", "UNTITLED"), false}
          });
    }

    @Test
    public void testIsTemporaryPath() {
      if (expected) {
        assertTrue(DatasetsUtil.isTemporaryPath(value));
      } else {
        assertFalse(DatasetsUtil.isTemporaryPath(value));
      }
    }
  }
}
