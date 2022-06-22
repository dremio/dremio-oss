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

  private static ResolverTestResults executeTest(String queryCorpus) {
    assert queryCorpus != null;

    ImmutableList<DremioToken> tokenizedCorpus = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(queryCorpus));
    TokenResolver.Predictions predictions = TokenResolver.getNextPossibleTokens(tokenizedCorpus);
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
        .map(token -> token.getImage())
        .toArray(String[]::new);

      boolean hasMoreResults = (predictions.getKeywords().size() - numCompletions) > 0;
      return new ResolverTestResults(
        completionStrings,
        predictions.isIdentifierPossible(),
        hasMoreResults);
    }
  }
}
