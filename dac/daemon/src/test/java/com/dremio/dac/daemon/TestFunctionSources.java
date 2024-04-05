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
package com.dremio.dac.daemon;

import static org.junit.Assert.assertTrue;

import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.Function;
import com.dremio.exec.expr.fn.FunctionInitializer;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/** Test that function source files are present in the classpath */
public class TestFunctionSources extends ExecTest {

  @Test
  public void testFunctions() {
    final List<String> notFound = new ArrayList<>();
    final Set<Class<? extends Function>> functions =
        ExecTest.CLASSPATH_SCAN_RESULT.getImplementations(Function.class);

    for (Class<? extends Function> function : functions) {
      try {
        FunctionInitializer.getSourceURL(function);
      } catch (IllegalArgumentException e) {
        notFound.add(function.getName());
      }
    }

    assertTrue(
        "The source files for the following functions are not present in the classpath: "
            + Joiner.on(",").join(notFound),
        notFound.isEmpty());
  }
}
