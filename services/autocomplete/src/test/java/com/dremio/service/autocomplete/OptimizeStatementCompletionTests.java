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
package com.dremio.service.autocomplete;

import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;

public class OptimizeStatementCompletionTests extends AutocompleteEngineTests {
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add("OPTIMIZE", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE ^"))
      .add("OPTIMIZE + PARTIAL TABLE", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TA^"))
      .add("OPTIMIZE + TABLE", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TABLE ^"))
      .add("OPTIMIZE + TABLE + TABLE NAME", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TABLE EMP ^"))
      .add("OPTIMIZE + TABLE + TABLE NAME + PAREN", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TABLE EMP (^"))
      .add("OPTIMIZE + TABLE + TABLE NAME + OPTION", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TABLE EMP (MIN_INPUT_FILES=5 ^"))
      .add("OPTIMIZE + TABLE + TABLE NAME + MULTIPLE OPTIONS", GoldenFileTestBuilder.MultiLineString.create("OPTIMIZE TABLE EMP (MIN_INPUT_FILES=5 , ^"))
      .runTests();
  }
}
