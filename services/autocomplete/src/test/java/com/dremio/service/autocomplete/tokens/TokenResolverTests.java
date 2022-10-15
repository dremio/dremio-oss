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
package com.dremio.service.autocomplete.tokens;

import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Test for the SQL autocomplete resolver.
 */
public final class TokenResolverTests {
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("EMPTY STRING", "")
      .add("SELECT", "SELECT ")
      .add("SELECT STAR", "SELECT * ")
      .add("SELECT STAR FROM", "SELECT * FROM ")
      .add("SELECT STAR FROM IDENTIFIER", "SELECT * FROM emp ")
      .add("SELECT STAR FROM IDENTIFIER WHERE", "SELECT * FROM emp WHERE ")
      .add("SELECT STAR FROM IDENTIFIER WHERE IDENTIFIER", "SELECT * FROM emp WHERE age ")
      .add("FUNCTION", "SELECT ABS(")
      .add("JOIN ON ", "SELECT * FROM EMP JOIN DEPT ON ")
      .runTests();
  }

  @Test
  public void multiSql() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add(
        "MULTI SQL",
        "SELECT * FROM EMP; ^")
      .add(
        "MULTI SQL 2",
        "SELECT * FROM EMP; SELECT ^")
      .add(
        "MULTI SQL 3",
        "SELECT * FROM ^;SELECT * FROM EMP")
      .add(
        "MULTI SQL 4",
        "SELECT * FROM ^;")
      .runTests();
  }

  @Test
  public void testDDL() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("SHOW", "SHOW ")
      .add("EXPLAIN", "EXPLAIN ")
      .add("DROP", "DROP ")
      .add("TRUNCATE", "TRUNCATE ")
      .add("CREATE", "CREATE ")
      .add("CREATE TABLE", "CREATE TABLE ")
      .add("CREATE VIEW", "CREATE VIEW ")
      .runTests();
  }

  @Test
  public void testSubquery() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("AFTER OPEN PARENS", "SELECT ( ")
      .add("IN", "SELECT * FROM EMP WHERE EMP.age IN (")
      .add("NOT IN", "SELECT * FROM EMP WHERE EMP.age NOT IN ( ")
      .add("DML", "UPDATE EMP SET age = age * 2 WHERE id IN (")
      .add("COMPARISON =", "SELECT CustomerID FROM Sales.Customer WHERE TerritoryID = (")
      .add("COMPARISON >", "SELECT Name FROM Production.Product WHERE ListPrice > (")
      .add("ANY", "SELECT Name FROM Production.Product WHERE ListPrice >= ANY ")
      .add("ANY(", "SELECT Name FROM Production.Product WHERE ListPrice >= ANY(")
      .add("SOME", "SELECT Name FROM Production.Product WHERE ListPrice >= SOME ")
      .add("SOME(", "SELECT Name FROM Production.Product WHERE ListPrice >= SOME(")
      .add("ALL", "SELECT Name FROM Production.Product WHERE ListPrice >= ALL ")
      .add("ALL(", "SELECT Name FROM Production.Product WHERE ListPrice >= ALL(")
      .add("EXISTS", "SELECT Name FROM Production.Product WHERE EXISTS ")
      .add("EXISTS(", "SELECT Name FROM Production.Product WHERE EXISTS(")
      .add("NOT EXISTS", "SELECT Name FROM Production.Product WHERE NOT EXISTS ")
      .add("NOT EXISTS", "SELECT Name FROM Production.Product WHERE NOT EXISTS(")
      .add("UNIQUE", "SELECT Name FROM Production.Product WHERE UNIQUE ")
      .add("UNIQUE(", "SELECT Name FROM Production.Product WHERE UNIQUE(")
      .add("ALIAS", "SELECT Name, ListPrice, (SELECT AVG(ListPrice) FROM Production.Product) ")
      .runTests();
  }

  @Test
  public void testNessie() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("JUST FINISHED TABLE NAME", "SELECT * FROM EMP ")
      .add("JUST FINISHED TABLE NAME WITH AT", "SELECT * FROM EMP AT ")
      .add("BRANCH", "SELECT * FROM EMP AT BRANCH ")
      .add("COMMIT", "SELECT * FROM EMP AT COMMIT ")
      .add("TAG","SELECT * FROM EMP AT TAG ")
      .runTests();
  }

  @Test
  public void comparisonOperators() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("Equals", "SELECT * FROM EMP WHERE EMP.age = ")
      .add("Not equal", "SELECT * FROM EMP WHERE EMP.age <> ")
      .add("Not equal", "SELECT * FROM EMP WHERE EMP.age != ")
      .add("Greater than", "SELECT * FROM EMP WHERE EMP.age > ")
      .add("Greater than or equal", "SELECT * FROM EMP WHERE EMP.age >= ")
      .add("Less than", "SELECT * FROM EMP WHERE EMP.age < ")
      .add("Less than or equal", "SELECT * FROM EMP WHERE EMP.age <= ")
      .add("Whether two values are equal, treating null values as the same", "SELECT * FROM EMP WHERE EMP.age <=> ")
      .add("IS", "SELECT * FROM EMP WHERE EMP.age IS ")
      .add("IS NULL", "SELECT * FROM EMP WHERE EMP.age IS NULL")
      .add("IS NOT", "SELECT * FROM EMP WHERE EMP.age IS NOT")
      .add("IS NOT NULL", "SELECT * FROM EMP WHERE EMP.age IS NOT NULL")
      .add("IS DISTINCT", "SELECT * FROM EMP WHERE EMP.age IS DISTINCT")
      .add("IS DISTINCT FROM", "SELECT * FROM EMP WHERE EMP.age IS DISTINCT FROM ")
      .add("IS NOT DISTINCT", "SELECT * FROM EMP WHERE EMP.age IS NOT DISTINCT ")
      .add("IS NOT DISTINCT FROM ", "SELECT * FROM EMP WHERE EMP.age IS NOT DISTINCT FROM ")
      .add("BETWEEN", "SELECT * FROM EMP WHERE EMP.age BETWEEN")
      .add("BETWEEN value2", "SELECT * FROM EMP WHERE EMP.age BETWEEN 25 ")
      .add("BETWEEN value2 AND", "SELECT * FROM EMP WHERE EMP.age BETWEEN 25 AND ")
      .add("NOT BETWEEN", "SELECT * FROM EMP WHERE EMP.age NOT BETWEEN")
      .add("NOT BETWEEN value2", "SELECT * FROM EMP WHERE EMP.age NOT BETWEEN 25 ")
      .add("NOT BETWEEN value2 AND", "SELECT * FROM EMP WHERE EMP.age NOT BETWEEN 25 AND ")
      .add("LIKE", "SELECT * FROM EMP WHERE EMP.age LIKE ")
      .add("LIKE string 2", "SELECT * FROM EMP WHERE EMP.name LIKE 'asdf*'")
      .add("NOT LIKE", "SELECT * FROM EMP WHERE EMP.age NOT LIKE ")
      .add("NOT LIKE string 2", "SELECT * FROM EMP WHERE EMP.name NOT LIKE 'asdf*'")
      .add("SIMILAR ", "SELECT * FROM EMP WHERE EMP.name SIMILAR ")
      .add("SIMILAR TO", "SELECT * FROM EMP WHERE EMP.name SIMILAR TO ")
      .add("SIMILAR TO string2", "SELECT * FROM EMP WHERE EMP.name SIMILAR TO 'asdf*'")
      .add("NOT SIMILAR ", "SELECT * FROM EMP WHERE EMP.name NOT SIMILAR ")
      .add("NOT SIMILAR TO", "SELECT * FROM EMP WHERE EMP.name NOT SIMILAR TO ")
      .add("NOT SIMILAR TO string2", "SELECT * FROM EMP WHERE EMP.name NOT SIMILAR TO 'asdf*'")
      .add("IN", "SELECT * FROM EMP WHERE EMP.name IN ")
      .add("IN(", "SELECT * FROM EMP WHERE EMP.name IN(")
      .add("IN(value", "SELECT * FROM EMP WHERE EMP.name IN('alice'")
      .add("IN(value,", "SELECT * FROM EMP WHERE EMP.name IN('alice', ")
      .add("IN(value)", "SELECT * FROM EMP WHERE EMP.name IN('alice')")
      .add("NOT IN", "SELECT * FROM EMP WHERE EMP.name NOT IN ")
      .add("NOT IN(", "SELECT * FROM EMP WHERE EMP.name NOT IN(")
      .add("NOT IN(value", "SELECT * FROM EMP WHERE EMP.name NOT IN('alice'")
      .add("NOT IN(value,", "SELECT * FROM EMP WHERE EMP.name NOT IN('alice', ")
      .add("NOT IN(value)", "SELECT * FROM EMP WHERE EMP.name NOT IN('alice')")
      .runTests();
  }

  @Test
  public void logicalOperators() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("OR", "SELECT * FROM EMP WHERE EMP.age = 5 OR ")
      .add("AND", "SELECT * FROM EMP WHERE EMP.age = 5 AND  ")
      .add("NOT", "SELECT * FROM EMP WHERE NOT ")
      .add("IS", "SELECT * FROM EMP WHERE EMP.blah IS ")
      .add("IS FALSE", "SELECT * FROM EMP WHERE EMP.blah IS FALSE ")
      .add("IS NOT", "SELECT * FROM EMP WHERE EMP.blah IS NOT ")
      .add("IS NOT FALSE", "SELECT * FROM EMP WHERE EMP.blah IS NOT FALSE ")
      .add("IS TRUE", "SELECT * FROM EMP WHERE EMP.blah IS TRUE ")
      .add("IS NOT TRUE", "SELECT * FROM EMP WHERE EMP.blah IS NOT TRUE ")
      .add("IS UNKNOWN", "SELECT * FROM EMP WHERE EMP.blah IS UNKNOWN ")
      .add("IS NOT UNKNOWN", "SELECT * FROM EMP WHERE EMP.blah IS NOT UNKNOWN ")
      .runTests();
  }

  @Test
  public void arithmeticOperators() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("positive", "SELECT +")
      .add("negative", "SELECT -")
      .add("plus", "SELECT EMP.age + ")
      .add("minus", "SELECT EMP.age - ")
      .add("multiplication", "SELECT EMP.age * ")
      .add("division", "SELECT EMP.age / ")
      .add("mod", "SELECT EMP.age % ")
      .runTests();
  }

  @Test
  public void characterOperators() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("concat", "SELECT 'asdf' || ")
      .runTests();
  }

  @Test
  public void reflections() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("ALTER + CREATE", "ALTER DATASET blah CREATE ")
      // RAW REFLECTIONS
      .add("RAW REFLECTION", "ALTER DATASET blah CREATE RAW REFLECTION ")
      .add("USING", "ALTER DATASET blah CREATE RAW REFLECTION USING ")
      .add("DISPLAY", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY")
      .add("DISPLAY(", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(")
      .add("DISPLAY(FIELD", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD")
      .add("USING DISPLAY(FIELD,", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD,")
      .add("DISTRIBUTE", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) DISTRIBUTE")
      .add("DISTRIBUTE BY", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) DISTRIBUTE BY")
      .add("PARTITION", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) PARTITION")
      .add("PARTITION BY", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) PARTITION BY")
      .add("LOCALSORT", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) LOCALSORT")
      .add("LOCALSORT BY", "ALTER DATASET blah CREATE RAW REFLECTION USING DISPLAY(FIELD) LOCALSORT BY")
      // AGGREGATE REFLECTIONS
      .add("DIMENSION", "ALTER TABLE BLAH \n" +
        " CREATE AGGREGATE REFLECTION BLAH \n" +
        " USING \n" +
        " DIMENSIONS (x ")
      .add("MEASURES", "ALTER TABLE BLAH \n" +
        " CREATE AGGREGATE REFLECTION BLAH \n" +
        " USING \n" +
        " DIMENSIONS (x by day, y) \n" +
        " MEASURES (b (")
      .runTests();
  }

  @Test
  public void specialSyntaxFunctions() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("CAST", "SELECT CAST(")
      .add("CAST", "SELECT CAST(myValue ")
      .add("CAST", "SELECT CAST(myValue AS")
      .add("CAST", "SELECT CAST(myValue AS INTEGER")
      .add("CAST", "SELECT CAST(myValue AS INTEGER)")

      .add("EXTRACT", "SELECT EXTRACT(")
      .add("EXTRACT", "SELECT EXTRACT(CENTURY")
      .add("EXTRACT", "SELECT EXTRACT(CENTURY FROM")
      .add("EXTRACT", "SELECT EXTRACT(CENTURY FROM datetimeColumn")
      .add("EXTRACT", "SELECT EXTRACT(CENTURY FROM datetimeColumn)")

      .add("POSITION", "SELECT POSITION(")
      .add("POSITION", "SELECT POSITION(string1")
      .add("POSITION", "SELECT POSITION(string1 IN")
      .add("POSITION", "SELECT POSITION(string1 IN string2")
      .add("POSITION", "SELECT POSITION(string1 IN string2)")
      .add("POSITION", "SELECT POSITION(string1 IN string2 FROM")
      .add("POSITION", "SELECT POSITION(string1 IN string2 FROM integer1")
      .add("POSITION", "SELECT POSITION(string1 IN string2 FROM integer1)")

      .add("CONVERT", "SELECT CONVERT(")
      .add("CONVERT", "SELECT CONVERT(myValue")
      .add("CONVERT", "SELECT CONVERT(myValue USING")
      .add("CONVERT", "SELECT CONVERT(myValue USING myValue2")
      .add("CONVERT", "SELECT CONVERT(myValue USING myValue2)")

      .add("TRANSLATE", "SELECT TRANSLATE(")
      .add("TRANSLATE", "SELECT TRANSLATE(char_value")
      .add("TRANSLATE", "SELECT TRANSLATE(char_value USING ")
      .add("TRANSLATE", "SELECT TRANSLATE(char_value USING translation_name")
      .add("TRANSLATE", "SELECT TRANSLATE(char_value USING translation_name)")
      .add("TRANSLATE", "SELECT TRANSLATE(inputString, characters")
      .add("TRANSLATE", "SELECT TRANSLATE(inputString, characters, translations")
      .add("TRANSLATE", "SELECT TRANSLATE(inputString, characters, translations)")

      .add("OVERLAY", "SELECT OVERLAY(")
      .add("OVERLAY", "SELECT OVERLAY(string1")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM integer1")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM integer1)")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM integer1 FOR ")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM integer1 FOR integer2")
      .add("OVERLAY", "SELECT OVERLAY(string1 PLACING string2 FROM integer1 FOR integer2)")

      .add("FLOOR", "SELECT FLOOR(")
      .add("FLOOR", "SELECT FLOOR(datetimeColumn")
      .add("FLOOR", "SELECT FLOOR(datetimeColumn TO")
      .add("FLOOR", "SELECT FLOOR(datetimeColumn TO CENTURY")
      .add("FLOOR", "SELECT FLOOR(datetimeColumn TO CENTURY)")

      .add("CEIL", "SELECT CEIL(")
      .add("CEIL", "SELECT CEIL(datetimeColumn")
      .add("CEIL", "SELECT CEIL(datetimeColumn TO")
      .add("CEIL", "SELECT CEIL(datetimeColumn TO CENTURY")
      .add("CEIL", "SELECT CEIL(datetimeColumn TO CENTURY)")

      .add("SUBSTRING", "SELECT SUBSTRING(")
      .add("SUBSTRING", "SELECT SUBSTRING(string1")
      .add("SUBSTRING", "SELECT SUBSTRING(string1 FROM")
      .add("SUBSTRING", "SELECT SUBSTRING(string1 FROM integer1")
      .add("SUBSTRING", "SELECT SUBSTRING(string1 FROM integer1 FOR ")
      .add("SUBSTRING", "SELECT SUBSTRING(string1 FROM integer1 FOR integer2")
      .add("SUBSTRING", "SELECT SUBSTRING(string1 FROM integer1 FOR integer2)")

      .add("TRIM", "SELECT TRIM(")
      .add("TRIM", "SELECT TRIM(string1")
      .add("TRIM", "SELECT TRIM(string1 FROM")
      .add("TRIM", "SELECT TRIM(string1 FROM string2")
      .add("TRIM", "SELECT TRIM(string1 FROM string2)")
      .add("TRIM", "SELECT TRIM(BOTH")
      .add("TRIM", "SELECT TRIM(BOTH string1")
      .add("TRIM", "SELECT TRIM(BOTH string1 FROM")
      .add("TRIM", "SELECT TRIM(BOTH string1 FROM string2")
      .add("TRIM", "SELECT TRIM(BOTH string1 FROM string2)")
      .add("TRIM", "SELECT TRIM(LEADING")
      .add("TRIM", "SELECT TRIM(LEADING string1")
      .add("TRIM", "SELECT TRIM(LEADING string1 FROM")
      .add("TRIM", "SELECT TRIM(LEADING string1 FROM string2")
      .add("TRIM", "SELECT TRIM(LEADING string1 FROM string2)")
      .add("TRIM", "SELECT TRIM(TRAILING")
      .add("TRIM", "SELECT TRIM(TRAILING string1")
      .add("TRIM", "SELECT TRIM(TRAILING string1 FROM")
      .add("TRIM", "SELECT TRIM(TRAILING string1 FROM string2")
      .add("TRIM", "SELECT TRIM(TRAILING string1 FROM string2)")

      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY, ")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY, myInteger")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY, myInteger, ")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY, myInteger, myDatetime")
      .add("TIMESTAMPADD", "SELECT TIMESTAMPADD(DAY, myInteger, myDatetime)")

      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY, ")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY, myDatetime")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY, myDatetime, ")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY, myDatetime, myDatetime2")
      .add("TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(DAY, myDatetime, myDatetime2)")

      .add("CASE", "SELECT CASE")
      .add("CASE", "SELECT CASE case1 WHEN")
      .add("CASE", "SELECT CASE case1 WHEN when1")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1 END")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1 ELSE")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1 ELSE resultZ")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1 ELSE resultZ END")
      .add("CASE", "SELECT CASE case1 WHEN when1, when2")
      .add("CASE", "SELECT CASE case1 WHEN when1, when2 THEN")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1")
      .add("CASE", "SELECT CASE case1 WHEN when1 THEN result1 THEN")

      .add("CASE", "SELECT CASE")
      .add("CASE", "SELECT CASE WHEN condition1")
      .add("CASE", "SELECT CASE WHEN condition1 THEN")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 END")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 ELSE")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 ELSE resultZ")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 ELSE resultZ END")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 WHEN ")
      .add("CASE", "SELECT CASE WHEN condition1 THEN result1 WHEN condition2")

      .add("COUNT", "SELECT COUNT(")
      .add("COUNT", "SELECT COUNT(*")
      .add("COUNT", "SELECT COUNT(*)")
      .add("COUNT", "SELECT COUNT(DISTINCT")
      .add("COUNT", "SELECT COUNT(DISTINCT myValue")
      .add("COUNT", "SELECT COUNT(DISTINCT myValue)")
      .add("COUNT", "SELECT COUNT(myValue")
      .add("COUNT", "SELECT COUNT(myValue)")
      .runTests();
  }

  @Test
  public void collectionFunctions() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      // MULTISET CONSTRUCTION
      .add("MULTISET CONSTRUCTION", "SELECT MULTISET")
      .add("MULTISET CONSTRUCTION SUBQUERY", "SELECT MULTISET(")
      .add("MULTISET CONSTRUCTION ARRAY", "SELECT MULTISET[")

      // ELEMENT
      .add("ELEMENT", "SELECT ELEMENT")
      .add("ELEMENT", "SELECT ELEMENT(")

      // CARDINALITY
      .add("CARDINALITY", "SELECT CARDINALITY")
      .add("CARDINALITY", "SELECT CARDINALITY(")

      // value MEMBER OF multiset
      .add("myValue", "SELECT myValue")
      .add("myValue MEMBER", "SELECT myValue MEMBER")
      .add("myValue MEMBER OF", "SELECT myValue MEMBER OF")
      .add("myValue MEMBER OF myMultiset", "SELECT myValue MEMBER OF myMultiset")

      // multiset IS A SET
      .add("multiset", "SELECT myMultiset")
      .add("multiset IS", "SELECT myMultiset IS")
      .add("multiset IS A", "SELECT myMultiset IS A")
      .add("multiset IS A SET", "SELECT myMultiset IS A SET")

      // multiset IS NOT A SET
      .add("multiset", "SELECT myMultiset")
      .add("multiset IS", "SELECT myMultiset IS")
      .add("multiset IS NOT", "SELECT myMultiset IS NOT ")
      .add("multiset IS NOT A", "SELECT myMultiset IS NOT A")
      .add("multiset IS NOT A SET", "SELECT myMultiset IS NOT A SET")

      // multiset IS EMPTY
      .add("multiset", "SELECT myMultiset")
      .add("multiset IS", "SELECT myMultiset IS")
      .add("multiset IS EMPTY", "SELECT myMultiset IS EMPTY")

      // multiset IS NOT EMPTY
      .add("multiset", "SELECT myMultiset")
      .add("multiset IS", "SELECT myMultiset IS")
      .add("multiset IS NOT", "SELECT myMultiset IS NOT")
      .add("multiset IS NOT EMPTY", "SELECT myMultiset IS NOT EMPTY")

      // multiset SUBMULTISET OF multiset2
      .add("multiset", "SELECT myMultiset")
      .add("multiset SUBMULTISET", "SELECT myMultiset SUBMULTISET")
      .add("multiset SUBMULTISET OF", "SELECT myMultiset SUBMULTISET OF")
      .add("multiset SUBMULTISET OF multiset2", "SELECT myMultiset SUBMULTISET OF myMultiset2")

      // multiset NOT SUBMULTISET OF multiset2
      .add("multiset", "SELECT myMultiset")
      .add("multiset NOT", "SELECT myMultiset NOT")
      .add("multiset NOT SUBMULTISET", "SELECT myMultiset NOT SUBMULTISET")
      .add("multiset NOT SUBMULTISET OF", "SELECT myMultiset NOT SUBMULTISET OF")
      .add("multiset NOT SUBMULTISET OF multiset2", "SELECT myMultiset NOT SUBMULTISET OF myMultiset2")

      // multiset MULTISET UNION [ ALL | DISTINCT ] multiset2
      .add("multiset", "SELECT myMultiset")
      .add("multiset MULTISET", "SELECT myMultiset MULTISET")
      .add("multiset MULTISET UNION ", "SELECT myMultiset MULTISET UNION")
      .add("multiset MULTISET UNION multiset2", "SELECT myMultiset MULTISET UNION myMultiset2")
      .add("multiset MULTISET UNION ALL", "SELECT myMultiset MULTISET UNION ALL")
      .add("multiset MULTISET UNION ALL multiset2", "SELECT myMultiset MULTISET UNION ALL myMultiset2")
      .add("multiset MULTISET UNION DISTINCT multiset2", "SELECT myMultiset MULTISET UNION DISTINCT")
      .add("multiset MULTISET UNION DISTINCT multiset2", "SELECT myMultiset MULTISET UNION DISTINCT myMultiset2")

      // multiset MULTISET INTERSECT [ ALL | DISTINCT ] multiset2
      .add("multiset", "SELECT myMultiset")
      .add("multiset MULTISET", "SELECT myMultiset MULTISET")
      .add("multiset MULTISET INTERSECT", "SELECT myMultiset MULTISET INTERSECT")
      .add("multiset MULTISET INTERSECT multiset2", "SELECT myMultiset MULTISET INTERSECT myMultiset2")
      .add("multiset MULTISET INTERSECT ALL", "SELECT myMultiset MULTISET INTERSECT ALL")
      .add("multiset MULTISET INTERSECT ALL multiset2", "SELECT myMultiset MULTISET INTERSECT ALL myMultiset2")
      .add("multiset MULTISET INTERSECT DISTINCT", "SELECT myMultiset MULTISET INTERSECT DISTINCT")
      .add("multiset MULTISET INTERSECT DISTINCT multiset2", "SELECT myMultiset MULTISET INTERSECT DISTINCT myMultiset2")

      // multiset MULTISET EXCEPT [ ALL | DISTINCT ] multiset2
      .add("multiset", "SELECT myMultiset")
      .add("multiset MULTISET", "SELECT myMultiset MULTISET")
      .add("multiset MULTISET EXCEPT", "SELECT myMultiset MULTISET EXCEPT")
      .add("multiset MULTISET EXCEPT multiset2", "SELECT myMultiset MULTISET EXCEPT myMultiset2")
      .add("multiset MULTISET EXCEPT ALL", "SELECT myMultiset MULTISET EXCEPT ALL")
      .add("multiset MULTISET EXCEPT ALL multiset2", "SELECT myMultiset MULTISET EXCEPT ALL myMultiset2")
      .add("multiset MULTISET EXCEPT DISTINCT", "SELECT myMultiset MULTISET EXCEPT DISTINCT")
      .add("multiset MULTISET EXCEPT DISTINCT multiset2", "SELECT myMultiset MULTISET EXCEPT DISTINCT myMultiset2")

      .runTests();
  }

  @Test
  public void periodPredicates() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      // PERIOD CONSTRUCTION
      .add("(", "SELECT (")
      .add("(datetime", "SELECT (datetime")
      .add("(datetime,", "SELECT (datetime, ")
      .add("(datetime, datetime", "SELECT (datetime, datetime")
      .add("(datetime, datetime)", "SELECT (datetime, datetime)")
      .add("PERIOD(", "SELECT PERIOD(")
      .add("PERIOD(datetime", "SELECT PERIOD(datetime")
      .add("PERIOD(datetime,", "SELECT PERIOD(datetime, ")
      .add("PERIOD(datetime, datetime", "SELECT PERIOD(datetime, datetime")
      .add("PERIOD(datetime, datetime)", "SELECT PERIOD(datetime, datetime)")

      // FUNCTIONS
      .add("CONTAINS", "SELECT myPeriod CONTAINS")
      .add("OVERLAPS", "SELECT myPeriod OVERLAPS")
      .add("EQUALS", "SELECT myPeriod EQUALS")
      .add("PRECEDES", "SELECT myPeriod PRECEDES")
      .add("IMMEDIATELY PRECEDES", "SELECT myPeriod IMMEDIATELY")
      .add("IMMEDIATELY PRECEDES", "SELECT myPeriod IMMEDIATELY PRECEDES")
      .add("SUCCEEDS", "SELECT myPeriod SUCCEEDS")
      .add("IMMEDIATELY SUCCEEDS", "SELECT myPeriod IMMEDIATELY")
      .add("IMMEDIATELY SUCCEEDS", "SELECT myPeriod IMMEDIATELY SUCCEEDS")
      .runTests();
  }

  @Test
  public void valueOperations() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      // Index is not recommended and close bracket is not high enough priority
      .add("ROW", "SELECT ROW")
      .add("ROW CONSTRUCTOR", "SELECT ROW(")
      .add("ROW CONSTRUCTOR", "SELECT ROW(1")
      .add("ROW CONSTRUCTOR", "SELECT ROW(1)")
      .add("ROW CONSTRUCTOR", "SELECT ROW(1,")
      .add("ROW INDEX", "SELECT ROW(1)[")
      .add("ROW INDEX + INT", "SELECT ROW(1)[1")
      .add("ROW INDEX + INT", "SELECT ROW(1)[1]")
      .add("ROW INDEX + STRING", "SELECT ROW(1)['name'")
      .add("ROW INDEX + STRING", "SELECT ROW(1)['name']")
      .add("ARRAY", "SELECT ARRAY")
      .add("ARRAY CONSTRUCTOR PARENS", "SELECT ARRAY(")
      .add("ARRAY CONSTRUCTOR PARENS", "SELECT ARRAY(SELECT * FROM T")
      .add("ARRAY CONSTRUCTOR PARENS", "SELECT ARRAY(SELECT * FROM T)")
      .add("ARRAY CONSTRUCTOR PARENS", "SELECT ARRAY(SELECT * FROM T,")
      .add("ARRAY PARENS INDEX", "SELECT ARRAY(SELECT * FROM T)[")
      .add("ARRAY PARENS INDEX", "SELECT ARRAY(SELECT * FROM T)[1")
      .add("ARRAY PARENS INDEX", "SELECT ARRAY(SELECT * FROM T)[1]")
      .add("ARRAY CONSTRUCTOR BRACKET", "SELECT ARRAY[")
      .add("ARRAY CONSTRUCTOR BRACKET", "SELECT ARRAY[1")
      .add("ARRAY CONSTRUCTOR BRACKET", "SELECT ARRAY[1]")
      .add("ARRAY CONSTRUCTOR BRACKET", "SELECT ARRAY[1,")
      .add("ARRAY BRACKET INDEX", "SELECT ARRAY[1, 2, 3][")
      .add("ARRAY BRACKET INDEX", "SELECT ARRAY[1, 2, 3][1")
      .add("ARRAY BRACKET INDEX", "SELECT ARRAY[1, 2, 3][1]")
      .add("MAP ", "SELECT MAP")
      .add("MAP CONSTRUCTOR PARENS", "SELECT MAP(")
      .add("MAP CONSTRUCTOR PARENS", "SELECT MAP(SELECT * FROM T")
      .add("MAP CONSTRUCTOR PARENS", "SELECT MAP(SELECT * FROM T,")
      .add("MAP CONSTRUCTOR PARENS", "SELECT MAP(SELECT * FROM T)")
      .add("MAP PARENS INDEX", "SELECT MAP(SELECT * FROM T)[")
      .add("MAP PARENS INDEX", "SELECT MAP(SELECT * FROM T)['key'")
      .add("MAP PARENS INDEX", "SELECT MAP(SELECT * FROM T)['key']")
      .add("MAP CONSTRUCTOR BRACKET", "SELECT MAP[")
      .add("MAP CONSTRUCTOR BRACKET", "SELECT MAP['key'")
      .add("MAP CONSTRUCTOR BRACKET", "SELECT MAP['key', 1")
      .add("MAP CONSTRUCTOR BRACKET", "SELECT MAP['key', 1]")
      .add("MAP BRACKET INDEX", "SELECT MAP['key', 1][")
      .add("MAP BRACKET INDEX", "SELECT MAP['key', 1]['key'")
      .add("MAP BRACKET INDEX", "SELECT MAP['key', 1]['key']")
      .runTests();
  }


  private static ResolverTestResults executeTest(String queryCorpus) {
    assert queryCorpus != null;

    StringAndPos stringAndPos = SqlParserUtil.findPos(queryCorpus);
    if (stringAndPos.cursor == -1) {
      queryCorpus = queryCorpus + " ^";
      stringAndPos = SqlParserUtil.findPos(queryCorpus);
    }

    ImmutableList<DremioToken> tokens = Cursor.tokenizeWithCursor(
      stringAndPos.sql,
      stringAndPos.cursor);
    TokenResolver.Predictions predictions = TokenResolver.getNextPossibleTokens(tokens);
    return ResolverTestResults.create(predictions);
  }

  private static final class ResolverTestResults {
    public final String[] tokens;
    public final boolean hasIdentifier;
    public final boolean hasMoreResults;

    private ResolverTestResults(String[] tokens, boolean hasIdentifier, boolean hasMoreResults) {
      this.tokens = tokens;
      this.hasIdentifier = hasIdentifier;
      this.hasMoreResults = hasMoreResults;
    }

    public static ResolverTestResults create(TokenResolver.Predictions predictions) {
      final int numCompletions = 5;
      final String[] completionStrings = predictions
        .getKeywords()
        .stream()
        .limit(numCompletions)
        .map(kind -> NormalizedTokenDictionary.INSTANCE.indexToImage(kind))
        .toArray(String[]::new);

      boolean hasMoreResults = (predictions.getKeywords().size() - numCompletions) > 0;
      return new ResolverTestResults(
        completionStrings,
        predictions.isIdentifierPossible(),
        hasMoreResults);
    }
  }
}
