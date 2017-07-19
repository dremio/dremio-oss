/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static com.dremio.exec.store.SubSchemaWrapper.toTopLevelSchemaName;
import static com.dremio.exec.store.SubSchemaWrapper.toSubSchemaPath;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class TestSubSchemaWrapper {

  @Test
  public void parseAndUnparseSubSchemaPathAsTopLevelSchemaName() {
    helper(asList("abc"), "abc");
    helper(asList("abc", "cde"), "abc.cde");
    helper(asList("ab.c"), "'ab.c'");
    helper(asList("ab'c"), "'ab''c'");
    helper(asList("abc", "cde", "efg"), "abc.cde.efg");
    helper(asList("ab''.c", "cd'e", "ef.g"), "'ab''''.c'.'cd''e'.'ef.g'");
  }

  private static void helper(List<String> subSchemaPath, String expTopLevelSchemaName) {
    final String actTopLevelSchemaName = toTopLevelSchemaName(subSchemaPath);
    assertEquals(expTopLevelSchemaName, actTopLevelSchemaName);
    assertEquals(subSchemaPath, toSubSchemaPath(actTopLevelSchemaName));
  }
}
