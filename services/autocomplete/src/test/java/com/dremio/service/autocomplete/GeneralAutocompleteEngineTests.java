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

public final class GeneralAutocompleteEngineTests extends AutocompleteEngineTests {
  @Test
  public void multiSql() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "MULTI SQL",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP; ^"))
      .add(
        "MULTI SQL 2",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP; SELECT ^"))
      .add(
        "MULTI SQL 3",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM ^;SELECT * FROM EMP"))
      .add(
        "MULTI SQL 4",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM ^;"))
      .runTests();
  }

  @Test
  public void prefixFiltering() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "PREFIX FILTERING FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT ONE_ARG_NUM^"))
      .add(
        "PREFIX FILTERING CATALOG ENTRIES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM dep^"))
      .add(
        "PREFIX FILTERING COLUMNS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT E^ FROM EMP"))
      .add(
        "PREFIX FILTERING KEYWORDS",
        GoldenFileTestBuilder.MultiLineString.create("S^"))
      .runTests();
  }

  @Test
  public void keywordOrFunction() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "BINARY FUNCTIONS SHOULD SURFACE AS KEYWORDS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE EMP.EMPNO ^"))
      .add(
        "NO DUPLICATES FOR SYSTEM FUNCTIONS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUN^ FROM EMP"))
      .add(
        "LEFT AS KEYWORD",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP LEF^"))
      .add(
        "LEFT AS FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT LEF^ FROM EMP"))
      .add(
        "ABS IS ONLY EVER A FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT AB^ FROM EMP"))
      .runTests();
  }

  @Test
  public void sqlStateMachine() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "INSIDE OF COMMENT BLOCK",
        GoldenFileTestBuilder.MultiLineString.create("--SELECT ^"))
      .add(
        "OUTSIDE OF COMMENT BLOCK",
        GoldenFileTestBuilder.MultiLineString.create("/*SELECT */ SELECT * ^"))
      .runTests();
  }

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "EMPTY STRING",
        GoldenFileTestBuilder.MultiLineString.create("^"))
      .runTests();
  }
}
