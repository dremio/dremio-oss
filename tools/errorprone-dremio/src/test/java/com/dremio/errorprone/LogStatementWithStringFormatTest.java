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
            "import org.slf4j.Logger;\n"
                + "import org.slf4j.LoggerFactory;\n"
                + "\n"
                + "public class Test {\n"
                + "    private final Logger logger = LoggerFactory.getLogger(getClass());\n"
                + "    void method() {\n"
                + "        // BUG: Diagnostic contains: Do not use String.format when calling a logging method\n"
                + "        logger.info(String.format(\"hello %s\", \"world\"));"
                + "    }\n"
                + "}")
        .doTest();
  }

  @Test
  public void testMarker() {
    helper
        .addSourceLines(
            "Test.java",
            "import org.slf4j.Logger;\n"
                + "import org.slf4j.LoggerFactory;\n"
                + "import org.slf4j.MarkerFactory;\n"
                + "import org.slf4j.Marker;\n"
                + "\n"
                + "public class Test {\n"
                + "    private final Logger logger = LoggerFactory.getLogger(getClass());\n"
                + "    private final Marker marker = MarkerFactory.getMarker(\"Sample\");\n"
                + "\n"
                + "    void method() {\n"
                + "        // BUG: Diagnostic contains: Do not use String.format when calling a logging method\n"
                + "        logger.warn(marker, String.format(\"hello %s\", \"world\"));"
                + "    }\n"
                + "}")
        .doTest();
  }
}
