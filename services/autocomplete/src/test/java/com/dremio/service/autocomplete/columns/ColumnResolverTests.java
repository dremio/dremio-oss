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
package com.dremio.service.autocomplete.columns;

import static com.dremio.service.autocomplete.catalog.mock.MockMetadataCatalog.createCatalog;

import java.util.Set;

import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.junit.Test;

import com.dremio.service.autocomplete.QueryParserFactory;
import com.dremio.service.autocomplete.catalog.mock.MockAutocompleteSchemaProvider;
import com.dremio.service.autocomplete.catalog.mock.MockMetadataCatalog;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.statements.grammar.Expression;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests for context aware auto completions.
 */
public final class ColumnResolverTests {
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithFolderContext)
      .add(
        "SIMPLE FROM CLAUSE",
        "SELECT ^ FROM EMP")
      .add(
        "FROM CLAUSE WITH COMMAS",
        "SELECT ^ FROM EMP, DEPT")
      .add(
        "FROM CLAUSE WITH JOINS",
        "SELECT ^ FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO")
      .runTests();
  }

  @Test
  public void aliasing() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithFolderContext)
      .add(
        "NO ALIAS",
        "SELECT ^ FROM EMP")
      .add(
        "BASIC ALIAS",
        "SELECT ^ FROM EMP as e")
      .add(
        "JOIN ALIAS COMMA",
        "SELECT ^ FROM EMP as e, DEPT as d")
      .add(
        "JOIN ALIAS ON",
        "SELECT ^ FROM EMP as e JOIN DEPT as d ON e.DEPTNO = d.DEPTNO")
      .add(
        "SELF JOIN",
        "SELECT ^ FROM EMP as e1, EMP as e2")
      .add(
        "SUBQUERY NO ALIAS",
        "SELECT ^ FROM (SELECT EMP.ENAME, EMP.DEPTNO FROM EMP)")
      .add(
        "SUBQUERY TABLE ALIAS",
        "SELECT ^ FROM (SELECT EMP.ENAME, EMP.DEPTNO FROM EMP) as subtable")
      .add(
        "SUBQUERY COLUMN ALIASES",
        "SELECT ^ FROM (SELECT EMP.ENAME as a, EMP.DEPTNO as b FROM EMP)")
      .add(
        "SUBQUERY COLUMN AND TABLE ALIASES",
        "SELECT ^ FROM (SELECT EMP.ENAME as a, EMP.DEPTNO as b FROM EMP) as subtable")
      .add(
        "SUBQUERY WITH JOIN",
        "SELECT ^ FROM (SELECT EMP.ENAME as a, EMP.DEPTNO as b, DEPT.DEPTNO as c FROM EMP, DEPT) as joinedTable")
      .runTests();
  }

  @Test
  public void aliasingWithHomeContext() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithHomeContext)
      .add(
        "NO ALIAS",
        "SELECT ^ FROM \"space\".\"folder\".EMP")
      .add(
        "BASIC ALIAS",
        "SELECT ^ FROM \"space\".\"folder\".EMP as e")
      .add(
        "JOIN ALIAS COMMA",
        "SELECT ^ FROM \"space\".\"folder\".EMP as e, \"space\".\"folder\".DEPT as d")
      .add(
        "SELF JOIN",
        "SELECT ^ FROM \"space\".\"folder\".EMP as e1, \"space\".\"folder\".EMP as e2")
      .add(
        "JOIN ALIAS ON",
        "SELECT ^ FROM \"space\".\"folder\".EMP as e JOIN \"space\".\"folder\".DEPT as d ON e.DEPTNO = d.DEPTNO")
      .runTests();
  }

  @Test
  public void subquery() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithFolderContext)
      .add(
        "CURSOR IN MIDDLE QUERY",
        "SELECT DEPTNO, (SELECT ^ , (SELECT * FROM SALGRADE) FROM EMP) FROM DEPT")
      .add(
        "CURSOR IN INNER QUERY",
        "SELECT DEPTNO, (SELECT MAX(EMP.SAL), ^ FROM EMP) FROM DEPT")
      .add(
        "CURSOR IN OUTER QUERY",
        "SELECT DEPTNO, ^ , (SELECT MAX(EMP.SAL) FROM EMP) FROM DEPT")
      .add(
        "CURSOR IN MIDDLE QUERY",
        "SELECT DEPTNO, (SELECT ^ , (SELECT * FROM SALGRADE) FROM EMP) FROM DEPT")
      .add(
        "SUBQUERY AS FROM CLAUSE",
        "SELECT * FROM (SELECT ^ FROM EMP)")
      .runTests();
  }

  @Test
  public void nessie() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithFolderContext)
      .add(
        "BRANCH",
        "SELECT ^ FROM EMP AT BRANCH \"Branch A\"")
      .add(
        "COMMIT",
        "SELECT ^ FROM EMP AT COMMIT \"DEADBEEF\"")
      .add(
        "TAG",
        "SELECT ^ FROM EMP AT TAG \"Tag A\"")
      .add(
        "WITH ALIAS",
        "SELECT ^ FROM EMP AT BRANCH \"Branch A\" AS EMPATBRANCH")
      .add(
        "MULTIPLE ATS",
        "SELECT ^ FROM EMP AT BRANCH \"Branch A\", DEPT AT BRANCH \"Branch B\"")
      .add(
        "SUBQUERY",
        "SELECT * FROM (SELECT ^ FROM EMP AT BRANCH \"Branch A\") as subtable")
      .runTests();
  }

  @Test
  public void nessieWithHomeContext() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTestWithHomeContext)
      .add(
        "BRANCH",
        "SELECT ^ FROM \"space\".\"folder\".EMP AT BRANCH \"Branch A\"")
      .add(
        "COMMIT",
        "SELECT ^ FROM \"space\".\"folder\".EMP AT COMMIT \"DEADBEEF\"")
      .add(
        "TAG",
        "SELECT ^ FROM \"space\".\"folder\".EMP AT TAG \"Tag A\"")
      .add(
        "WITH ALIAS",
        "SELECT ^ FROM \"space\".\"folder\".EMP AT BRANCH \"Branch A\" AS EMPATBRANCH")
      .add(
        "MULTIPLE ATS",
        "SELECT ^ FROM \"space\".\"folder\".EMP AT BRANCH \"Branch A\", \"space\".\"folder\".DEPT AT BRANCH \"Branch B\"")
      .add(
        "SUBQUERY",
        "SELECT ^ FROM (SELECT * FROM \"space\".\"folder\".EMP AT Branch \"Branch A\") as subtable")
      .runTests();
  }

  private static Set<ColumnAndTableAlias> executeTestWithHomeContext(String query) {
    ImmutableList<String> context = ImmutableList.of();
    return executeTest(createCatalog(context), query);
  }

  private static Set<ColumnAndTableAlias> executeTestWithFolderContext(String query) {
    ImmutableList<String> context = ImmutableList.of("space", "folder");
    return executeTest(createCatalog(context), query);
  }

  private static Set<ColumnAndTableAlias> executeTest(MockMetadataCatalog.CatalogData data, String query) {
    SqlNodeParser dremioQueryParser = QueryParserFactory.create(new MockMetadataCatalog(data));
    ColumnResolver columnResolver = new ColumnResolver(
      new MockAutocompleteSchemaProvider(
        data.getContext(),
        data.getHead()),
      dremioQueryParser);

    StringAndPos stringAndPos = SqlParserUtil.findPos(query);
    Expression expression = (Expression) Cursor.extractElementWithCursor(
      Statement.parse(
        stringAndPos.sql,
        stringAndPos.cursor));

    Set<ColumnAndTableAlias> columnAndTableAliases = columnResolver.resolve(expression.getTableReferences());
    return columnAndTableAliases;
  }
}
