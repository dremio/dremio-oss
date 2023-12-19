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

public class ImplementBothUnwrapAndIsWrapperForTest {
  private CompilationTestHelper compilationHelper;

  @Before
  public void setup() {
    compilationHelper =
        CompilationTestHelper.newInstance(ImplementBothUnwrapAndIsWrapperFor.class, getClass());
  }

  @Test
  public void testHasBoth() {
    compilationHelper
        .addSourceLines(
            "HasBoth.java",
            "import com.dremio.common.Wrapper;",
            "public class HasBoth implements Wrapper {",
            "  @Override",
            "  public <T> T unwrap(Class<T> clazz) {",
            "    if (isWrapperFor(clazz)) {",
            "      return clazz.cast(this);",
            "    }",
            "    throw new IllegalArgumentException(",
            "      String.format(\"This object (type '%s') cannot be unwrapped into '%s'\", getClass(), clazz));",
            "  }",
            "  @Override",
            "  public boolean isWrapperFor(Class<?> clazz) {",
            "    return clazz.isInstance(this);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void testHasUnwrapButNotIsWrapperFor() {
    compilationHelper
        .addSourceLines(
            "HasUnwrapButNotIsWrapperFor.java",
            "import com.dremio.common.Wrapper;",
            "// BUG: Diagnostic contains: [ImplementBothUnwrapAndIsWrapperFor] Class HasUnwrapButNotIsWrapperFor implements unwrap but does not implement isWrapperFor.",
            "public class HasUnwrapButNotIsWrapperFor implements Wrapper {",
            "  @Override",
            "  public <T> T unwrap(Class<T> clazz) {",
            "    if (isWrapperFor(clazz)) {",
            "      return clazz.cast(this);",
            "    }",
            "    throw new IllegalArgumentException(",
            "      String.format(\"This object (type '%s') cannot be unwrapped into '%s'\", getClass(), clazz));",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void testHasIsWrapperForButNotUnwrap() {
    compilationHelper
        .addSourceLines(
            "HasIsWrapperForButNotUnwrap.java",
            "import com.dremio.common.Wrapper;",
            "// BUG: Diagnostic contains:  [ImplementBothUnwrapAndIsWrapperFor] Class HasIsWrapperForButNotUnwrap implements isWrapperFor but does not implement unwrap.",
            "public class HasIsWrapperForButNotUnwrap implements Wrapper {",
            "  @Override",
            "  public boolean isWrapperFor(Class<?> clazz) {",
            "    return clazz.isInstance(this);",
            "  }",
            "}")
        .doTest();
  }

  @Test
  public void testHasOnlyOneButDoesNotImplementWrapper() {
    compilationHelper
        .addSourceLines(
            "HasOnlyOneButDoesNotImplementWrapper.java",
            "public class HasOnlyOneButDoesNotImplementWrapper {",
            "  public boolean isWrapperFor(Class<?> clazz) {",
            "    return clazz.isInstance(this);",
            "  }",
            "}")
        .doTest();
  }
}
