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
package com.dremio.exec.expr.fn;

import com.dremio.test.DremioTest;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class TestHiveFunctionRegistry extends DremioTest {

  @Test
  public void printHiveRegistryFunctions() {
    HiveFunctionRegistry registry = new HiveFunctionRegistry(CLASSPATH_SCAN_RESULT);
    Set<String> allFuncs = new HashSet<>(registry.getMethodsUDF().keySet());
    allFuncs.addAll(registry.getMethodsGenericUDF().keySet());
    allFuncs.stream().map(String::toLowerCase).sorted().forEach(System.out::println);
  }
}
