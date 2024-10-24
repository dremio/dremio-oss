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

public class LogStatementWithStringFormatTest {
  private CompilationTestHelper helper;

  @Before
  public void setup() {
    helper = CompilationTestHelper.newInstance(LogStatementWithStringFormat.class, getClass());
  }

  @Test
  public void testSimple() {
    helper
        .addSourceLines(
            "Test.java",
            "import org.slf4j.Logger;",
            "import org.slf4j.LoggerFactory;",
            "",
            "public class Test {",
            "    private final Logger logger = LoggerFactory.getLogger(getClass());",
            "    void method() {",
            "        // BUG: Diagnostic contains:",
            "        logger.info(String.format(\"hello %s\", \"world\"));",
            "    }",
            "}")
        .doTest();
  }

  @Test
  public void testMarker() {
    helper
        .addSourceLines(
            "Test.java",
            "import org.slf4j.Logger;",
            "import org.slf4j.LoggerFactory;",
            "import org.slf4j.MarkerFactory;",
            "import org.slf4j.Marker;",
            "",
            "public class Test {",
            "    private final Logger logger = LoggerFactory.getLogger(getClass());",
            "    private final Marker marker = MarkerFactory.getMarker(\"Sample\");",
            "",
            "    void method() {",
            "        // BUG: Diagnostic contains:",
            "        logger.warn(marker, String.format(\"hello %s\", \"world\"));",
            "    }",
            "}")
        .doTest();
  }
}
