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
package com.dremio.errorprone;

import com.google.errorprone.CompilationTestHelper;
import org.junit.Before;
import org.junit.Test;

public class NoGuavaCacheUsageTest {
  private CompilationTestHelper helper;

  @Before
  public void setup() {
    helper = CompilationTestHelper.newInstance(NoGuavaCacheUsage.class, getClass());
  }

  @Test
  public void testNoCacheBuilderBuild() {
    helper
        .addSourceLines(
            "Test.java",
            "import com.google.common.cache.CacheBuilder;",
            "import com.google.common.cache.CacheLoader;",
            "",
            "public class Test {",
            "    void simpleBuild() {",
            "        // BUG: Diagnostic contains: Use caffeine cache instead of guava cache",
            "        CacheBuilder.newBuilder().build();",
            "    }",
            "",
            "    void loaderBuild() {",
            "        CacheLoader<String, String> loader = null;",
            "        // BUG: Diagnostic contains: Use caffeine cache instead of guava cache",
            "        CacheBuilder.newBuilder().build(loader);",
            "    }",
            "",
            "    private static class MyCacheLoader extends CacheLoader<String, String> {",
            "        @Override",
            "        public String load(String key) {",
            "            return key;",
            "        }",
            "    }",
            "",
            "    void customLoaderBuild() {",
            "        // BUG: Diagnostic contains: Use caffeine cache instead of guava cache",
            "        CacheBuilder.newBuilder().build(new MyCacheLoader());",
            "    }",
            "}")
        .doTest();
  }
}
