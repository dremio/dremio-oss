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
package com.dremio.service.autocomplete.functions;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;

public class FunctionSnippetFactoryTests {
  @Test
  public void tests() {
    List<String> names = FunctionDictionary
      .INSTANCE
      .getKeys()
      .stream()
      .sorted()
      .collect(Collectors.toList());
    new GoldenFileTestBuilder<>(FunctionSnippetFactoryTests::executeTest)
      .addListByRule(names, (name) -> Pair.of(name,name))
      .runTests();
  }

  private static List<String> executeTest(String functionName) {
    return FunctionDictionary
      .INSTANCE
      .tryGetValue(functionName)
      .get()
      .getSignatures()
      .stream()
      .map(signature -> FunctionSnippetFactory.create(functionName, signature))
      .map(snippet -> snippet.toString())
      .collect(Collectors.toList());
  }
}
