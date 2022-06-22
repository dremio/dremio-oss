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

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;

public final class DeleteStatementCompletionTests extends AutocompleteEngineTests {
  @Ignore("DX-50485")
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add("DELETE", GoldenFileTestBuilder.MultiLineString.create("DELETE ^"))
      .add("DELETE + PARTIAL FROM", GoldenFileTestBuilder.MultiLineString.create("DELETE FR^"))
      .add("DELETE + FROM", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM ^"))
      .add("DELETE + FROM + TABLE", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM EMP ^"))
      .add("DELETE + FROM + TABLE + AS", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM EMP AS ^"))
      .add("DELETE + FROM + TABLE + AS + ALIAS", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM EMP AS e ^"))
      .add("DELETE + FROM + TABLE + WHERE", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM EMP WHERE ^"))
      .add("DELETE + FROM + TABLE + WHERE + CONDITION", GoldenFileTestBuilder.MultiLineString.create("DELETE FROM EMP WHERE EMP.NAME = 'Brandon' ^"))
      .runTests();
  }
}
