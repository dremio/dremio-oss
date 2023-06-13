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

import static com.dremio.plugins.dataplane.store.SearchQueryToCelConversionUtilities.likeQueryToRe2Regex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.dremio.service.catalog.SearchQuery;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

class TestSearchQueryToCelConversionUtilities {

  private static final Set<Character> SOME_REGULAR_CHARACTERS = ImmutableSet.of('a', 'A', 'z', 'Z', '-', '_');
  private static final ImmutableMap<Object, Object> SEARCH_QUERY_SPECIAL_CHARACTERS_MAP =
    ImmutableMap.builder()
      .put('%', ".*")
      .build();
  // CEL uses RE2 syntax: https://github.com/google/re2/wiki/Syntax
  private static final Set<Character> RE2_SPECIAL_CHARACTERS =
    ImmutableSet.of('*', '+', '?', '(', ')', '|', '[', ']', ':', '^', '\\', '.', '{', '}');

  private static Stream<Arguments> convertToRawCelStringLiteralArguments() {
    return Stream.of(
      Arguments.of(
        "basicString",
        "r'basicString'"),
      Arguments.of(
        "string'withRawSingleQuote",
        "r'string''withRawSingleQuote'"),
      Arguments.of(
        "string%withSpecialCharacter",
        "r'string%withSpecialCharacter'"),
      Arguments.of(
        "string\\withBackslash",
        "r'string\\withBackslash'"));
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("convertToRawCelStringLiteralArguments")
  public void convertToRawCelStringLiteral(String pattern, String expected) {
    assertThat(SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(pattern))
      .isEqualTo(expected);
  }

  @Test
  public void likeQueryToRe2RegexEmptyPattern() {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern("")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo("^$");
  }

  @Test
  public void likeQueryToRe2RegexBasicString() {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern("basicString")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo("^basicString$");
  }

  @Test
  public void likeQueryToRe2RegexOnlyEscape() {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern("\\")
      .setEscape("\\")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo("^$");
  }

  private static Stream<Arguments> regularCharacters() {
    return SOME_REGULAR_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          c.toString(),               // e.g. a
          String.format("^%c$", c))); // e.g. ^a$ (keep the regular character)
  }

  private static Stream<Arguments> escapedRegularCharacters(char escape) {
    return SOME_REGULAR_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          String.format("%c%c", escape, c), // e.g. \a
          String.format("^%c$", c)));       // e.g. ^a$ (ignore escape, keep the regular character)
  }

  private static Stream<Arguments> escapedRegularCharactersBackslash() {
    return escapedRegularCharacters('\\');
  }

  private static Stream<Arguments> escapedRegularCharactersBacktick() {
    return escapedRegularCharacters('`');
  }

  private static Stream<Arguments> escapedRegularCharactersNoEscape() {
    return SOME_REGULAR_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          String.format("\\%c", c),       // e.g. \a
          String.format("^\\\\%c$", c))); // e.g. ^\\a$ (escape backslash, keep the regular character)
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("regularCharacters")
  public void likeQueryToRe2RegexRegularCharacters(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRegularCharactersBackslash")
  public void likeQueryToRe2RegexEscapedRegularCharactersBackslash(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("\\")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRegularCharactersBacktick")
  public void likeQueryToRe2RegexEscapedRegularCharactersBacktick(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("`")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRegularCharactersNoEscape")
  public void likeQueryToRe2RegexEscapedRegularCharactersNoEscape(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  private static Stream<Arguments> searchQuerySpecialCharacters() {
    return SEARCH_QUERY_SPECIAL_CHARACTERS_MAP.entrySet().stream()
      .map(entry ->
        Arguments.of(
          String.format("%s", entry.getKey().toString()), // e.g. %
          String.format("^%s$", entry.getValue())));      // e.g. ^.*$ (convert the SearchQuery special character)
  }

  private static Stream<Arguments> escapedSearchQuerySpecialCharacters(char escape) {
    return SEARCH_QUERY_SPECIAL_CHARACTERS_MAP.keySet().stream()
      .map(key -> Arguments.of(
        String.format("%c%s", escape, key.toString()), // e.g. \%
        String.format("^%s$", key)));                  // e.g. ^%$ (include the raw SearchQuery special character)
    /* Note: We only have a single SearchQuery special character (%). It's not
     * one of the RE2 Regex special characters. If we ever have a special
     * character that is in both SearchQuery and RE2 Regex special characters,
     * then this test's expected value needs to change to accommodate.
     */
  }

  private static Stream<Arguments> escapedSearchQuerySpecialCharactersBackslash() {
    return escapedSearchQuerySpecialCharacters('\\');
  }

  private static Stream<Arguments> escapedSearchQuerySpecialCharactersBacktick() {
    return escapedSearchQuerySpecialCharacters('`');
  }

  private static Stream<Arguments> escapedSearchQuerySpecialCharactersNoEscape() {
    return SEARCH_QUERY_SPECIAL_CHARACTERS_MAP.entrySet().stream()
      .map(entry ->
        Arguments.of(
          String.format("\\%s", entry.getKey().toString()), // e.g. \%
          String.format("^\\\\%s$", entry.getValue())));    // e.g. ^\\.*$ (escape backslash, convert the SearchQuery special character)
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("searchQuerySpecialCharacters")
  public void likeQueryToRe2RegexSearchQuerySpecialCharacters(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedSearchQuerySpecialCharactersBackslash")
  public void likeQueryToRe2RegexEscapedSearchQuerySpecialCharactersBackslash(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("\\")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedSearchQuerySpecialCharactersBacktick")
  public void likeQueryToRe2RegexEscapedSearchQuerySpecialCharactersBacktick(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("`")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedSearchQuerySpecialCharactersNoEscape")
  public void likeQueryToRe2RegexEscapedSearchQuerySpecialCharactersNoEscape(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  private static Stream<Arguments> re2SpecialCharacters() {
    return RE2_SPECIAL_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          String.format("%c", c),       // e.g. *
          String.format("^\\%c$", c))); // e.g. ^\*$ (escape the re2 special character)
  }

  private static Stream<Arguments> escapedRe2SpecialCharacters(char escape) {
    return RE2_SPECIAL_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          String.format("%c%c", escape, c), // e.g. \*
          String.format("^\\%c$", c)));     // e.g. ^\*$ (ignore escape, escape the re2 special character)
  }

  private static Stream<Arguments> escapedRe2SpecialCharactersBackslash() {
    return escapedRe2SpecialCharacters('\\');
  }

  private static Stream<Arguments> escapedRe2SpecialCharactersBacktick() {
    return escapedRe2SpecialCharacters('`');
  }

  private static Stream<Arguments> escapedRe2SpecialCharactersNoEscape() {
    return RE2_SPECIAL_CHARACTERS.stream()
      .map(c ->
        Arguments.of(
          String.format("\\%c", c),         // e.g. \*
          String.format("^\\\\\\%c$", c))); // e.g. ^\\\*$ (escape backslash, escape the re2 special character)
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("re2SpecialCharacters")
  public void likeQueryToRe2RegexRe2SpecialCharacters(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRe2SpecialCharactersBackslash")
  public void likeQueryToRe2RegexEscapedRe2SpecialCharactersBackslash(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("\\")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRe2SpecialCharactersBacktick")
  public void likeQueryToRe2RegexEscapedRe2SpecialCharactersBacktick(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .setEscape("`")
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @ParameterizedTest(name = "[{index}] pattern: [{0}], expected: [{1}]")
  @MethodSource("escapedRe2SpecialCharactersNoEscape")
  public void likeQueryToRe2RegexEscapedRe2SpecialCharactersNoEscape(String pattern, String expectedRegex) {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern(pattern)
      .build();
    assertThat(likeQueryToRe2Regex(likeQuery)).isEqualTo(expectedRegex);
  }

  @Test
  public void likeQueryToRe2RegexInvalidEscape() {
    final SearchQuery.Like likeQuery = SearchQuery.Like.newBuilder()
      .setPattern("basicString")
      .setEscape("12") // Two characters
      .build();
    assertThatThrownBy(() -> likeQueryToRe2Regex(likeQuery))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
