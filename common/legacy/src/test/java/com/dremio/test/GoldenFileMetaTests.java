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
package com.dremio.test;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;

/**
 * Tests to assert that the Golden File library is working as expected.
 */
public final class GoldenFileMetaTests  {
  @Test
  public void testSuccessScenario() {
    GoldenFileTestBuilder.<Input, Integer>create(input -> input.left + input.right)
      .add("3 plus 5", new Input(3, 5))
      .add("5 plus 8", new Input(5, 8))
      .runTests();
  }

  @Test
  public void testExpectedExceptionScenario() {
    GoldenFileTestBuilder.create(GoldenFileMetaTests::addWithException)
      .allowExceptions()
      .add("3 plus 5", new Input(3, 5))
      .add("5 plus 8", new Input(5, 8))
      .runTests();
  }

  @Test
  public void testUnexpectedExceptionScenario() {
    try {
      GoldenFileTestBuilder.create(GoldenFileMetaTests::addWithException)
          .add("3 plus 5", new Input(3, 5))
          .runTests();
      Assert.fail();
    } catch (RuntimeException ex) {
      Assert.assertEquals("Throwing an exception for testing purposes", ex.getCause().getMessage());
    }
  }

  @Test
  public void testIgnoreScenario() {
    GoldenFileTestBuilder.<Input, Integer>create(input -> input.left + input.right)
      .add("Correct Output And Ignore = false", new Input(3, 5))
      .addButIgnore("Correct Output And Ignore = true", new Input(3, 5))
      .addButIgnore("Incorrect Output And Ignore = true", new Input(3, 5))
      .runTests();
  }

  @Test(expected = ComparisonFailure.class)
  public void testIncorrectOutput() {
    GoldenFileTestBuilder.<Input, Integer>create(input -> input.left + input.right)
      .allowExceptions()
      .add("Incorrect Output And Ignore = false", new Input(3, 5))
      .runTests();
  }

  @SuppressWarnings("AssertionFailureIgnored")
  @Test
  public void testFirstRun() {
    try {
      GoldenFileTestBuilder.create((Integer i) -> i)
          .add("Example Test", 1)
          .runTests();
      Assert.fail();

    } catch (AssertionError error) {
      Assert.assertEquals(
          "Message should be that the count does not match",
          ""
              + "To fix:\n"
              + "\t`cp target/goldenfiles/actual/GoldenFileMetaTests.testFirstRun.yaml src/test/resources/goldenfiles/expected/GoldenFileMetaTests.testFirstRun.yaml`\n"
              + "To Diff:\n"
              + "\t`sdiff target/goldenfiles/actual/GoldenFileMetaTests.testFirstRun.yaml src/test/resources/goldenfiles/expected/GoldenFileMetaTests.testFirstRun.yaml`\n"
              + " expected:<0> but was:<1>",
          error.getMessage());
      Assert.assertTrue("The actual file is still initialized even though the test fails",
          Files.exists(Paths.get("target/goldenfiles/actual/GoldenFileMetaTests.testFirstRun.yaml") ));
    }
  }

  @Test
  public void testNotCasesAdded() {
    try {
      GoldenFileTestBuilder.create((Integer i) -> i)
          .runTests();
      Assert.fail();
    } catch (IllegalStateException error) {
      Assert.assertEquals(
          "Message should be that the count does not match",
          "No test cases found.",
          error.getMessage());
    }
  }

  private static Integer addWithException(Input input) throws Exception {
    throw new Exception("Throwing an exception for testing purposes");
  }

  private static final class Input  {
    public final int left;
    public final int right;

    public Input(int left, int right) {
      this.left = left;
      this.right = right;
    }
  }
}
