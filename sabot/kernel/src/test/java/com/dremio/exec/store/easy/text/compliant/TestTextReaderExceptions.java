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
package com.dremio.exec.store.easy.text.compliant;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.service.namespace.file.proto.TextFileConfig;

@RunWith(Parameterized.class)
public class TestTextReaderExceptions extends TestTextReaderHelper {

  private final ArrayList<Class<Throwable>> expectedThrowable = new ArrayList<>();

  private final String[] expectedMsg = new String[2];

  private static final String expectedRecordNotFound = "did not find expected record in result set";

  private static final String numOfRecordsDiffer = "Different number of records returned";

  public TestTextReaderExceptions(TextFileConfig fileFormat, String[][] expected, String testFileName, Class<Throwable> expectedThrowableForSelectAll, String expectedMsgForSelectAll, Class<Throwable> expectedThrowableForSelectCol, String expectedMsgForSelectCol) {
    super(fileFormat, expected, testFileName);
    this.expectedThrowable.add(expectedThrowableForSelectAll);
    this.expectedMsg[0] = expectedMsgForSelectAll;
    this.expectedThrowable.add(expectedThrowableForSelectCol);
    this.expectedMsg[1] = expectedMsgForSelectCol;
  }

  @Parameterized.Parameters(name = "{index}: test file: {2}, Corresponding Expected Messages for selectAll and selectcol: {4}, {6} , Table Options: {0} ")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]
        {
          {
            // Trim Header false
            new TextFileConfig().setLineDelimiter("\n").setTrimHeader(false),
            new String[][] {
              {" c1 "," c2 "," c3 "},
              {"r1c1","r1c2","r1c3"},
              {"r2c1","r2c2","r2c3"}
            },
            "trim_header.csv",
            AssertionError.class,
            "Unexpected column",
            Exception.class,
            "VALIDATION ERROR: Column 'c1' not found in any table"
          }
        }
    );
  }

  @Test
  public void testSelectQueryExceptions() {
    testSelectQueryExceptionsHelper(this::testTableOptionsSelectAll, expectedThrowable.get(0), expectedMsg[0]);
    testSelectQueryExceptionsHelper(this::testTableOptionsSelectCol, expectedThrowable.get(1), expectedMsg[1]);
  }

  private void testSelectQueryExceptionsHelper(ThrowingRunnable sqlTestBuilderFunc, Class<Throwable> expectedThrowableClass, String expectedMsg) {
    Throwable throwable = assertThrows("Expected to throw "+ expectedThrowableClass,
      expectedThrowableClass, sqlTestBuilderFunc);
    assertTrue("Expected Message and Thrown Message Differ",throwable.getMessage().contains(expectedMsg));
  }
}
