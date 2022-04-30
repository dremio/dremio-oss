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
package com.dremio.exec.compile;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.dremio.exec.expr.fn.FunctionInitializer;

public class TestMethodToLabel {
  private static final String TEST_CLAZZ_FQN_PREFIX = "com.dremio.exec.expr.fn.impl";
  private static final String TEST_CLAZZ_NAME = "CharSubstring";
  private static final String TEST_CLAZZ_FQN = TEST_CLAZZ_FQN_PREFIX + "." + TEST_CLAZZ_NAME;

  @Test
  public void testMethodToLabelledStatement() {
    FunctionInitializer initializer = new FunctionInitializer(TEST_CLAZZ_FQN);
    final String expectedLabel = TEST_CLAZZ_NAME + "_eval:";
    assertThat(initializer.getMethod("eval")).contains(expectedLabel);
    final String expectedLabel1 = TEST_CLAZZ_NAME + "_setup:";
    assertThat(initializer.getMethod("setup")).contains(expectedLabel1);
  }
}
