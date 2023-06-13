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
  public void prefixFiltering() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add("PREFIX FILTERING FUNCTION", "SELECT AB^")
      .add("PREFIX FILTERING PARAMETER", "SELECT ABS(EMP.DEPT^ FROM EMP")
      .add("PREFIX FILTERING CATALOG ENTRIES", "SELECT * FROM dep^")
      .add("PREFIX FILTERING CATALOG ENTRIES WITH DOUBLE QUOTES", "SELECT * FROM \"dep^\"")
      .add("PREFIX FILTERING COLUMNS", "SELECT E^ FROM EMP")
      .add("PREFIX FILTERING KEYWORDS", "S^")
      .runTests();
  }

  @Test
  public void keywordOrFunction() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add("BINARY FUNCTIONS SHOULD SURFACE AS KEYWORDS", "SELECT * FROM EMP WHERE EMP.EMPNO ^")
      .add("NO DUPLICATES FOR SYSTEM FUNCTIONS", "SELECT COUN^ FROM EMP")
      .add("LEFT AS KEYWORD", "SELECT * FROM EMP LEF^")
      .add("LEFT AS FUNCTION", "SELECT LEF^")
      .add("ABS IS ONLY EVER A FUNCTION", "SELECT AB^")
      .add("MIN AS FUNCTION", "SELECT MI^")
      .add(
        "MIN AS KEYWORD",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO BY DAY, ENAME)\n" +
          "MEASURES(EMPNO (COUNT, MI^")
      .runTests();
  }

  @Test
  public void sqlStateMachine() {
    GoldenFileTestBuilder.create(this::executeTestWithFolderContext)
      .add("INSIDE OF COMMENT BLOCK", "--SELECT ^")
      .add("OUTSIDE OF COMMENT BLOCK", "/*SELECT */ SELECT * ^")
      .add("INSIDE DOUBLE QUOTES", "SELECT * FROM \"^\"")
      .runTests();
  }

  @Test
  public void tests() {
    GoldenFileTestBuilder.create(this::executeTestWithFolderContext)
      .add("EMPTY STRING", "^")
      .runTests();
  }
}
