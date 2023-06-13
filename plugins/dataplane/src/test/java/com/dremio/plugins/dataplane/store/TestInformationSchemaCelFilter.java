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
package com.dremio.plugins.dataplane.store;

import static com.dremio.plugins.dataplane.store.InformationSchemaCelFilter.getInformationSchemaFilter;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.SearchQuery.And;
import com.dremio.service.catalog.SearchQuery.Equals;
import com.dremio.service.catalog.SearchQuery.Like;
import com.dremio.service.catalog.SearchQuery.Or;

public class TestInformationSchemaCelFilter {

  private static final String DATAPLANE_PLUGIN_NAME = "test_dataplane";
  private static final String TABLE_NAME = "test_table";
  private static final String FULL_PATH = "test_dataplane.folder1.folder2";
  private static final String SEARCH_NAME = "SEARCH_NAME";
  private static final String SEARCH_SCHEMA = "SEARCH_SCHEMA";
  private static final String QUOTED_PATH = "\"this.is.a.single.folder\".subfolder.table";

  @Test
  public void testBasicEqualSearchQueryToCel() {
    Equals equals = Equals.newBuilder()
      .setField(SEARCH_NAME)
      .setStringValue(TABLE_NAME)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setEquals(equals)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("(r'%s') == entry.name", TABLE_NAME));
  }

  @Test
  public void testBasicLikeSearchQueryToCel() {
    Like like = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%%%s%%", TABLE_NAME))
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("entry.name.matches(r'^.*%s.*$')", TABLE_NAME));
  }

  /**
   * Same Query as we load all schema from Tableau
   */
  @Test
  public void testTableauGetAllSchemata() {
    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(DATAPLANE_PLUGIN_NAME)
      .setEscape("\\")
      .build();

    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, true, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(null);

  }

  @Test
  public void testGetSchemataFullPath() {
    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();

    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, true, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("(r'test_dataplane' + '.' + entry.namespace + '.' + entry.name) == (r'%s')", FULL_PATH));
  }

  @Test
  public void testQueryNotSchemata() {
    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();

    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("(r'test_dataplane' + '.' + entry.namespace) == (r'%s')", FULL_PATH));
  }

  @Test
  public void testTableauGetExactTableName() {
    Like tableNameLike = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(TABLE_NAME)
      .setEscape("\\")
      .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder()
      .setLike(tableNameLike)
      .build();
    Like tablePathLike = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder()
      .setLike(tablePathLike)
      .build();
    And and = And.newBuilder()
      .addClauses(tableNameLikeQuery)
      .addClauses(tablePathLikeQuery)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setAnd(and)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("((entry.name.matches(r'^%s$')) && ((r'%s' + '.' + entry.namespace) == (r'%s')))", TABLE_NAME, DATAPLANE_PLUGIN_NAME, FULL_PATH));
  }

  @Test
  public void testTableauStartWith() {
    Like tableNameLike = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%s%%", TABLE_NAME))
      .setEscape("\\")
      .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder()
      .setLike(tableNameLike)
      .build();
    Like tablePathLike = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder()
      .setLike(tablePathLike)
      .build();
    And and = And.newBuilder()
      .addClauses(tableNameLikeQuery)
      .addClauses(tablePathLikeQuery)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setAnd(and)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("((entry.name.matches(r'^%s.*$')) && ((r'%s' + '.' + entry.namespace) == (r'%s')))", TABLE_NAME, DATAPLANE_PLUGIN_NAME, FULL_PATH));
  }

  @Test
  public void testTableauContains() {
    Like tableNameLike = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%%%s%%", TABLE_NAME))
      .setEscape("\\")
      .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder()
      .setLike(tableNameLike)
      .build();
    Like tablePathLike = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder()
      .setLike(tablePathLike)
      .build();
    And and = And.newBuilder()
      .addClauses(tableNameLikeQuery)
      .addClauses(tablePathLikeQuery)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setAnd(and)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("((entry.name.matches(r'^.*%s.*$')) && ((r'%s' + '.' + entry.namespace) == (r'%s')))", TABLE_NAME, DATAPLANE_PLUGIN_NAME, FULL_PATH));
  }

  @Test
  public void testComplexQuery() {
    Like tableNameLike = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%%%s%%", TABLE_NAME))
      .setEscape("\\")
      .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder()
      .setLike(tableNameLike)
      .build();
    Like tablePathLike = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(FULL_PATH)
      .setEscape("\\")
      .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder()
      .setLike(tablePathLike)
      .build();
    Equals tableNameEquals = Equals.newBuilder()
      .setField(SEARCH_NAME)
      .setStringValue(String.format("%%%s%%", TABLE_NAME))
      .build();
    SearchQuery tableNameEqualQuery = SearchQuery.newBuilder()
      .setEquals(tableNameEquals)
      .build();
    Equals tablePathEqual = Equals.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setStringValue(FULL_PATH)
      .build();
    SearchQuery tablePathEqualQuery = SearchQuery.newBuilder()
      .setEquals(tablePathEqual)
      .build();
    And and = And.newBuilder()
      .addClauses(tableNameLikeQuery)
      .addClauses(tablePathLikeQuery)
      .build();
    And and2 = And.newBuilder()
      .addClauses(tableNameEqualQuery)
      .addClauses(tablePathEqualQuery)
      .build();
    SearchQuery andQuery1 = SearchQuery.newBuilder()
      .setAnd(and)
      .build();
    SearchQuery andQuery2 = SearchQuery.newBuilder()
      .setAnd(and2)
      .build();
    Or or = Or.newBuilder()
      .addClauses(andQuery1)
      .addClauses(andQuery2)
      .build();
    SearchQuery searchQuery2 = SearchQuery.newBuilder()
      .setOr(or)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery2, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("((((entry.name.matches(r'^.*%s.*$')) && ((r'%s' + '.' + entry.namespace) == (r'%s')))) || ((((r'%%%s%%') == entry.name) && ((r'%s' + '.' + entry.namespace) == (r'%s')))))", TABLE_NAME, DATAPLANE_PLUGIN_NAME, FULL_PATH,TABLE_NAME, DATAPLANE_PLUGIN_NAME, FULL_PATH));
  }

  @Test
  public void testQuotedPathEqualsQuery() {
    Equals equals = Equals.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setStringValue(QUOTED_PATH)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setEquals(equals)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo("(r'test_dataplane' + '.' + entry.namespace) == (r'\"this.is.a.single.folder\".subfolder.table')");
  }

  @Test
  public void testQuotedPathLikeQuery() {
    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(QUOTED_PATH)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo("(r'test_dataplane' + '.' + entry.namespace) == (r'\"this.is.a.single.folder\".subfolder.table')");
  }

  @Test
  public void testInvalidQuery() {
    Equals equals = Equals.newBuilder()
      .setField("WRONG_FIELD")
      .setStringValue(QUOTED_PATH)
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setEquals(equals)
      .build();
    Assertions.assertThrows(IllegalStateException.class, () -> getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void testNullQuery() {
    assertThat(getInformationSchemaFilter(null, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(null);
  }

  @Test
  public void testStar() {
    String name = "*starTable";
    String expected = "\\*starTable";
    Like like = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%%%s%%", name))
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("entry.name.matches(r'^.*%s.*$')", expected));
  }

  @Test
  public void testQuestionMark() {
    String name = "?question?table?";
    String expected = "\\?question\\?table\\?";
    Like like = Like.newBuilder()
      .setField(SEARCH_NAME)
      .setPattern(String.format("%%%s%%", name))
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, false, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("entry.name.matches(r'^.*%s.*$')", expected));
  }

  /**
   * The folder has name "dot.dot.dot.dot" and it's under DATAPLANE_PLUGIN_NAME source.
   */
  @Test
  public void testDotsInQuotes() {
    String tableWithDotsInQuotes = "\"dot.dot.dot.dot.table\"";
    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(String.format("%s.%s", DATAPLANE_PLUGIN_NAME, tableWithDotsInQuotes))
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();
    assertThat(getInformationSchemaFilter(searchQuery, true, DATAPLANE_PLUGIN_NAME))
      .isEqualTo(String.format("(r'%s' + '.' + entry.name) == (r'%s.%s')", DATAPLANE_PLUGIN_NAME, DATAPLANE_PLUGIN_NAME, tableWithDotsInQuotes));
  }

  @Test
  public void testSchemataNested() {
    String sourceName = "DATAPLANE_PLUGIN_NAME";
    String tableName = "table1";

    Like like = Like.newBuilder()
      .setField(SEARCH_SCHEMA)
      .setPattern(String.format("'%s'.%s%%", sourceName, tableName))
      .setEscape("\\")
      .build();
    SearchQuery searchQuery = SearchQuery.newBuilder()
      .setLike(like)
      .build();

    String nameElements = String.format("r'%s' + '.' + entry.namespace + '.' + entry.name", sourceName);
    String nameOnly = String.format("r'%s' + '.' + entry.name", sourceName);
    String value = String.format("^''%s''\\.%s.*$", sourceName, tableName);

    assertThat(getInformationSchemaFilter(searchQuery, true, sourceName))
      .isEqualTo(String.format("(r'%s').matches(r'%s') || (%s).matches(r'%s') || (%s).matches(r'%s')", sourceName, value, nameElements, value, nameOnly, value));
  }
}
