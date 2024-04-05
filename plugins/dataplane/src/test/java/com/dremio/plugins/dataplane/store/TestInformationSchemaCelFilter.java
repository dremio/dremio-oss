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

import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.SearchQuery.And;
import com.dremio.service.catalog.SearchQuery.Equals;
import com.dremio.service.catalog.SearchQuery.Like;
import com.dremio.service.catalog.SearchQuery.Or;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestInformationSchemaCelFilter {

  private static final String DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN = "test_dataplane";
  private static final String TABLE_NAME_HAS_UNDERSCORE_PATTERN = "test_table";
  private static final String DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE =
      "test\\_dataplane";
  private static final String TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE = "test\\_table";
  private static final String FULL_PATH_HAS_UNDERSCORE_PATTERN = "test_dataplane.folder1.fold_r2";
  private static final String FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE =
      "test\\_dataplane.folder1.folder2";
  private static final String SEARCH_NAME = "SEARCH_NAME";
  private static final String SEARCH_SCHEMA = "SEARCH_SCHEMA";
  private static final String QUOTED_PATH = "\"this.is.a.single.folder\".subfolder.table";
  private static final String QUOTED_PATH_ESCAPED =
      "\"this\\.is\\.a\\.single\\.folder\"\\.subfolder\\.table";
  private static final Map<String, String> PATTERN_TO_MATCHER = new HashMap<>();

  @BeforeAll
  public static void setup() {
    PATTERN_TO_MATCHER.put(TABLE_NAME_HAS_UNDERSCORE_PATTERN, "test.table");
    PATTERN_TO_MATCHER.put(DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN, "test.dataplane");
    PATTERN_TO_MATCHER.put(FULL_PATH_HAS_UNDERSCORE_PATTERN, "test.dataplane\\.folder1\\.fold.r2");
    PATTERN_TO_MATCHER.put(
        FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE, "test_dataplane\\.folder1\\.folder2");
    PATTERN_TO_MATCHER.put(
        DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE, "test_dataplane");
    PATTERN_TO_MATCHER.put(TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE, "test_table");
  }

  private static Stream<Arguments> testBasicEqualsSearchQuery() {
    return Stream.of(
        Arguments.of(TABLE_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testBasicEqualsSearchQuery")
  public void testBasicEqualSearchQueryToCel(String equalValue) {
    Equals equals = Equals.newBuilder().setField(SEARCH_NAME).setStringValue(equalValue).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setEquals(equals).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(String.format("(r'%s') == entry.name", equalValue));
  }

  private static Stream<Arguments> testBasicLikeSearchQuery() {
    return Stream.of(
        Arguments.of(TABLE_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testBasicLikeSearchQuery")
  public void testBasicLikeSearchQueryToCel(String pattern) {
    Like like =
        Like.newBuilder()
            .setField(SEARCH_NAME)
            .setPattern(String.format("%%%s%%", pattern))
            .build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(
            String.format("entry.name.matches(r'^.*%s.*$')", PATTERN_TO_MATCHER.get(pattern)));
  }

  private static Stream<Arguments> testAllSchemata() {
    return Stream.of(
        Arguments.of(DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  /** Same Query as we load all schema from Tableau */
  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testAllSchemata")
  public void testTableauGetAllSchemata(String pattern) {
    Like like =
        Like.newBuilder().setField(SEARCH_SCHEMA).setPattern(pattern).setEscape("\\").build();

    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(getInformationSchemaFilter(searchQuery, true, pattern))
        .isEqualTo(
            String.format(
                "(r'%s' + '.' + entry.namespace + '.' + entry.name).matches(r'^%s$') || (r'%s' + '.' + entry.name).matches(r'^%s$') && entry.namespace == ''",
                pattern,
                PATTERN_TO_MATCHER.get(pattern),
                pattern,
                PATTERN_TO_MATCHER.get(pattern)));
  }

  private static Stream<Arguments> testSchemataFullPath() {
    return Stream.of(
        Arguments.of(
            FULL_PATH_HAS_UNDERSCORE_PATTERN, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("testSchemataFullPath")
  public void testGetSchemataFullPath(String likePattern, String source) {
    Like like =
        Like.newBuilder().setField(SEARCH_SCHEMA).setPattern(likePattern).setEscape("\\").build();

    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(getInformationSchemaFilter(searchQuery, true, source))
        .isEqualTo(
            String.format(
                "(r'%s' + '.' + entry.namespace + '.' + entry.name).matches(r'^%s$') || (r'%s' + '.' + entry.name).matches(r'^%s$') && entry.namespace == ''",
                source,
                PATTERN_TO_MATCHER.get(likePattern),
                source,
                PATTERN_TO_MATCHER.get(likePattern)));
  }

  private static Stream<Arguments> testNotSchemata() {
    return Stream.of(
        Arguments.of(
            FULL_PATH_HAS_UNDERSCORE_PATTERN, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("testNotSchemata")
  public void testQueryNotSchemata(String likePattern, String source) {
    Like like =
        Like.newBuilder().setField(SEARCH_SCHEMA).setPattern(likePattern).setEscape("\\").build();

    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(getInformationSchemaFilter(searchQuery, false, source))
        .isEqualTo(
            String.format(
                "((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')",
                source,
                PATTERN_TO_MATCHER.get(likePattern),
                source,
                PATTERN_TO_MATCHER.get(likePattern)));
  }

  private static Stream<Arguments> testExactTableName() {
    return Stream.of(
        Arguments.of(
            TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            FULL_PATH_HAS_UNDERSCORE_PATTERN,
            DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(
            TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("testExactTableName")
  public void testTableauGetExactTableName(
      String tableNamePattern, String fullPathPattern, String sourceName) {
    Like tableNameLike =
        Like.newBuilder()
            .setField(SEARCH_NAME)
            .setPattern(tableNamePattern)
            .setEscape("\\")
            .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder().setLike(tableNameLike).build();
    Like tablePathLike =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(fullPathPattern)
            .setEscape("\\")
            .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder().setLike(tablePathLike).build();
    And and =
        And.newBuilder().addClauses(tableNameLikeQuery).addClauses(tablePathLikeQuery).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setAnd(and).build();
    assertThat(getInformationSchemaFilter(searchQuery, false, sourceName))
        .isEqualTo(
            String.format(
                "((entry.name.matches(r'^%s$')) && (((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')))",
                PATTERN_TO_MATCHER.get(tableNamePattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern)));
  }

  private static Stream<Arguments> testStartsWith() {
    return Stream.of(
        Arguments.of(
            TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE),
        Arguments.of(
            TABLE_NAME_HAS_UNDERSCORE_PATTERN,
            FULL_PATH_HAS_UNDERSCORE_PATTERN,
            DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN));
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("testStartsWith")
  public void testTableauStartWith(
      String tableNamePattern, String fullPathPattern, String sourceName) {
    Like tableNameLike =
        Like.newBuilder()
            .setField(SEARCH_NAME)
            .setPattern(String.format("%s%%", tableNamePattern))
            .setEscape("\\")
            .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder().setLike(tableNameLike).build();
    Like tablePathLike =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(fullPathPattern)
            .setEscape("\\")
            .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder().setLike(tablePathLike).build();
    And and =
        And.newBuilder().addClauses(tableNameLikeQuery).addClauses(tablePathLikeQuery).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setAnd(and).build();
    assertThat(getInformationSchemaFilter(searchQuery, false, sourceName))
        .isEqualTo(
            String.format(
                "((entry.name.matches(r'^%s.*$')) && (((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')))",
                PATTERN_TO_MATCHER.get(tableNamePattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern)));
  }

  private static Stream<Arguments> testContains() {
    return Stream.of(
        Arguments.of(
            TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE),
        Arguments.of(
            TABLE_NAME_HAS_UNDERSCORE_PATTERN,
            FULL_PATH_HAS_UNDERSCORE_PATTERN,
            DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN));
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("testContains")
  public void testTableauContains(
      String tableNamePattern, String fullPathPattern, String sourceName) {
    Like tableNameLike =
        Like.newBuilder()
            .setField(SEARCH_NAME)
            .setPattern(String.format("%%%s%%", tableNamePattern))
            .setEscape("\\")
            .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder().setLike(tableNameLike).build();
    Like tablePathLike =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(fullPathPattern)
            .setEscape("\\")
            .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder().setLike(tablePathLike).build();
    And and =
        And.newBuilder().addClauses(tableNameLikeQuery).addClauses(tablePathLikeQuery).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setAnd(and).build();
    assertThat(getInformationSchemaFilter(searchQuery, false, sourceName))
        .isEqualTo(
            String.format(
                "((entry.name.matches(r'^.*%s.*$')) && (((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')))",
                PATTERN_TO_MATCHER.get(tableNamePattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern)));
  }

  private static Stream<Arguments> testComplexQueries() {
    return Stream.of(
        Arguments.of(
            TABLE_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            FULL_PATH_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE,
            DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE),
        Arguments.of(
            TABLE_NAME_HAS_UNDERSCORE_PATTERN,
            FULL_PATH_HAS_UNDERSCORE_PATTERN,
            DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN));
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("testComplexQueries")
  public void testComplexQuery(String tableNamePattern, String fullPathPattern, String sourceName) {
    Like tableNameLike =
        Like.newBuilder()
            .setField(SEARCH_NAME)
            .setPattern(String.format("%%%s%%", tableNamePattern))
            .setEscape("\\")
            .build();
    SearchQuery tableNameLikeQuery = SearchQuery.newBuilder().setLike(tableNameLike).build();
    Like tablePathLike =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(fullPathPattern)
            .setEscape("\\")
            .build();
    SearchQuery tablePathLikeQuery = SearchQuery.newBuilder().setLike(tablePathLike).build();
    Equals tableNameEquals =
        Equals.newBuilder()
            .setField(SEARCH_NAME)
            .setStringValue(String.format("%%%s%%", tableNamePattern))
            .build();
    SearchQuery tableNameEqualQuery = SearchQuery.newBuilder().setEquals(tableNameEquals).build();
    Equals tablePathEqual =
        Equals.newBuilder().setField(SEARCH_SCHEMA).setStringValue(fullPathPattern).build();
    SearchQuery tablePathEqualQuery = SearchQuery.newBuilder().setEquals(tablePathEqual).build();
    And and =
        And.newBuilder().addClauses(tableNameLikeQuery).addClauses(tablePathLikeQuery).build();
    And and2 =
        And.newBuilder().addClauses(tableNameEqualQuery).addClauses(tablePathEqualQuery).build();
    SearchQuery andQuery1 = SearchQuery.newBuilder().setAnd(and).build();
    SearchQuery andQuery2 = SearchQuery.newBuilder().setAnd(and2).build();
    Or or = Or.newBuilder().addClauses(andQuery1).addClauses(andQuery2).build();
    SearchQuery searchQuery2 = SearchQuery.newBuilder().setOr(or).build();
    assertThat(getInformationSchemaFilter(searchQuery2, false, sourceName))
        .isEqualTo(
            String.format(
                "((((entry.name.matches(r'^.*%s.*$')) && (((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')))) || ((((r'%%%s%%') == entry.name) && ((r'%s' + '.' + entry.namespace) == (r'%s')))))",
                PATTERN_TO_MATCHER.get(tableNamePattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern),
                sourceName,
                PATTERN_TO_MATCHER.get(fullPathPattern),
                tableNamePattern,
                sourceName,
                fullPathPattern));
  }

  @Test
  public void testQuotedPathEqualsQuery() {
    Equals equals = Equals.newBuilder().setField(SEARCH_SCHEMA).setStringValue(QUOTED_PATH).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setEquals(equals).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(
            "(r'test_dataplane' + '.' + entry.namespace) == (r'\"this.is.a.single.folder\".subfolder.table')");
  }

  @Test
  public void testQuotedPathLikeQuery() {
    Like like = Like.newBuilder().setField(SEARCH_SCHEMA).setPattern(QUOTED_PATH).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(
            String.format(
                "((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')",
                DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN,
                QUOTED_PATH_ESCAPED,
                DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN,
                QUOTED_PATH_ESCAPED));
  }

  @Test
  public void testInvalidQuery() {
    Equals equals = Equals.newBuilder().setField("WRONG_FIELD").setStringValue(QUOTED_PATH).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setEquals(equals).build();
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN));
  }

  @Test
  public void testNullQuery() {
    assertThat(
            getInformationSchemaFilter(null, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(null);
  }

  @Test
  public void testStar() {
    String name = "*starTable";
    String expected = "\\*starTable";
    Like like =
        Like.newBuilder().setField(SEARCH_NAME).setPattern(String.format("%%%s%%", name)).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(String.format("entry.name.matches(r'^.*%s.*$')", expected));
  }

  @Test
  public void testQuestionMark() {
    String name = "?question?table?";
    String expected = "\\?question\\?table\\?";
    Like like =
        Like.newBuilder().setField(SEARCH_NAME).setPattern(String.format("%%%s%%", name)).build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, false, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(String.format("entry.name.matches(r'^.*%s.*$')", expected));
  }

  /** The folder has name "dot.dot.dot.dot" and it's under DATAPLANE_PLUGIN_NAME source. */
  @Test
  public void testDotsInQuotes() {
    String tableWithDotsInQuotes = "\"dot.dot.dot.dot.table\"";
    String tableWithDotsInQuotesEscaped = "\"dot\\.dot\\.dot\\.dot\\.table\"";
    Like like =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(
                String.format(
                    "%s.%s", DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN, tableWithDotsInQuotes))
            .build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();
    assertThat(
            getInformationSchemaFilter(
                searchQuery, true, DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN))
        .isEqualTo(
            String.format(
                "(r'%s' + '.' + entry.namespace + '.' + entry.name).matches(r'^%s\\.%s$') || (r'%s' + '.' + entry.name).matches(r'^%s\\.%s$') && entry.namespace == ''",
                DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN,
                PATTERN_TO_MATCHER.get(DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
                tableWithDotsInQuotesEscaped,
                DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN,
                PATTERN_TO_MATCHER.get(DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
                tableWithDotsInQuotesEscaped));
  }

  private static Stream<Arguments> testNestedSchemata() {
    return Stream.of(
        Arguments.of(DATAPLANE_PLUGIN_NAME_HAS_UNDERSCORE_PATTERN),
        Arguments.of(DATAPLANE_PLUGIN_NAME_PATTERN_MATCHES_EXACTLY_WITH_UNDERSCORE));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testNestedSchemata")
  public void testSchemataNested(String sourceName) {
    String tableName = "table1";

    Like like =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern(String.format("'%s'.%s%%", sourceName, tableName))
            .setEscape("\\")
            .build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();

    String nameElements =
        String.format("r'%s' + '.' + entry.namespace + '.' + entry.name", sourceName);
    String nameOnly = String.format("r'%s' + '.' + entry.name", sourceName);
    String value = String.format("^''%s''\\.%s.*$", PATTERN_TO_MATCHER.get(sourceName), tableName);

    assertThat(getInformationSchemaFilter(searchQuery, true, sourceName))
        .isEqualTo(
            String.format(
                "(%s).matches(r'%s') || (%s).matches(r'%s') && entry.namespace == ''",
                nameElements, value, nameOnly, value));
  }

  @Test
  public void testCelFilterWithUnderScore() {
    String sourceName = "nessie_source";

    Like like =
        Like.newBuilder()
            .setField(SEARCH_SCHEMA)
            .setPattern("nessie\\_source") // same as r'(nessie_source)
            .setEscape("\\")
            .setCaseInsensitive(true)
            .build();
    SearchQuery searchQuery = SearchQuery.newBuilder().setLike(like).build();

    assertThat(getInformationSchemaFilter(searchQuery, false, sourceName))
        .isEqualTo(
            String.format(
                "((r'%s').matches(r'^%s$') && entry.namespace == '') || (r'%s' + '.' + entry.namespace).matches(r'^%s$')",
                sourceName, sourceName, sourceName, sourceName));
  }
}
