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

public final class ReflectionCreateCompletionTests extends AutocompleteEngineTests {
  @Test
  public void raw() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "USING + DISPLAY + OPEN",
        "ALTER TABLE EMP CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(^")
      .add(
        "USING + DISPLAY + DISPLAY FIELDS",
        "ALTER TABLE EMP CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(EMPNO, ^")
      .add(
        "DISTRIBUTE",
        "ALTER TABLE EMP CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(EMPNO, ENAME) \n" +
          "DISTRIBUTE BY(^")
      .add(
        "PARTITION",
        "ALTER TABLE EMP CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(EMPNO, ENAME) \n" +
          "PARTITION BY(^")
      .add(
        "LOCALSORT",
        "ALTER TABLE EMP CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(EMPNO, ENAME) \n" +
          "LOCALSORT BY(^")
      .runTests();
  }

  @Test
  public void aggregateReflectionCreateTests() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "DIMENSIONS FIELD",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(^)\n" +
          "MEASURES(EMPNO)")
      .add(
        "DIMENSIONS FIELD BY DAY",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(^ BY DAY)\n" +
          "MEASURES(EMPNO)")
      .add(
        "DIMENSIONS MIXED",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO BY DAY, ^)\n" +
          "MEASURES(EMPNO)")
      .add(
        "MEASURES FIELD",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO)\n" +
          "MEASURES(^)")
      .add(
        "MEASURES WITH ANNOTATIONS",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO)\n" +
          "MEASURES(^ (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT))")
      .add(
        "MEASURES WITH ANNOTATIONS2",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO)\n" +
          "MEASURES(EMPNO (COUNT, MIN, MAX, ^, APPROXIMATE COUNT DISTINCT))")
      .add(
        "MEASURES MIXED",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO)\n" +
          "MEASURES(EMPNO (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT), ^)")
      .add(
        "EVERYTHING",
        "ALTER TABLE EMP CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(EMPNO BY DAY, ENAME)\n" +
          "MEASURES(EMPNO (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT), ENAME)\n" +
          "DISTRIBUTE BY(EMPNO, ENAME)\n" +
          "PARTITION BY(^, ENAME)\n" +
          "LOCALSORT BY(EMPNO, ENAME)\n" +
          "ARROW CACHE true")
      .runTests();
  }

  @Test
  public void externalReflectionCreateTests() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "USING",
        "ALTER TABLE EMP CREATE EXTERNAL REFLECTION myReflection\n" +
          "USING ^")
      .runTests();
  }
}
