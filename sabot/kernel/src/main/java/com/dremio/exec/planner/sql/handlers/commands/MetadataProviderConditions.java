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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** Helper class to create filter and predicates for metadata queries */
public final class MetadataProviderConditions {

  static final Predicate<String> ALWAYS_TRUE = x -> true;

  private static final String SQL_LIKE_ANY_STRING_PATTERN = "%";

  private MetadataProviderConditions() {}

  public static Predicate<String> getTableTypePredicate(List<String> tableTypeFilter) {
    return tableTypeFilter.isEmpty() ? ALWAYS_TRUE : ImmutableSet.copyOf(tableTypeFilter)::contains;
  }

  public static Predicate<String> getCatalogNamePredicate(LikeFilter filter) {
    if (filter == null
        || !filter.hasPattern()
        || SQL_LIKE_ANY_STRING_PATTERN.equals(filter.getPattern())) {
      return ALWAYS_TRUE;
    }

    final String patternString =
        RegexpUtil.sqlToRegexLike(
            filter.getPattern(), filter.hasEscape() ? filter.getEscape().charAt(0) : (char) 0);
    final Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
    return input -> pattern.matcher(input).matches();
  }

  /**
   * Helper method to create a {@link SearchTypes.SearchQuery} that combines the given filters with
   * an AND.
   *
   * @param schemaNameFilter Optional filter on <code>schema name</code>
   * @param tableNameFilter Optional filter on <code>table name</code>
   * @return optional search query equivalent
   */
  public static Optional<SearchQuery> createConjunctiveQuery(
      LikeFilter schemaNameFilter, LikeFilter tableNameFilter) {
    final Optional<SearchQuery> schemaNameQuery =
        createLikeQuery(DatasetIndexKeys.UNQUOTED_SCHEMA.getIndexFieldName(), schemaNameFilter);
    final Optional<SearchQuery> tableNameQuery =
        createLikeQuery(DatasetIndexKeys.UNQUOTED_NAME.getIndexFieldName(), tableNameFilter);

    if (!schemaNameQuery.isPresent()) {
      return tableNameQuery;
    }

    //noinspection OptionalIsPresent
    if (!tableNameQuery.isPresent()) {
      return schemaNameQuery;
    }

    return Optional.of(
        SearchQuery.newBuilder()
            .setAnd(
                SearchQuery.And.newBuilder()
                    .addClauses(schemaNameQuery.get())
                    .addClauses(tableNameQuery.get()))
            .build());
  }

  private static Optional<SearchQuery> createLikeQuery(String fieldName, LikeFilter likeFilter) {
    if (likeFilter == null
        || !likeFilter.hasPattern()
        || SQL_LIKE_ANY_STRING_PATTERN.equals(likeFilter.getPattern())) {
      return Optional.empty();
    }

    final String escape = likeFilter.hasEscape() ? likeFilter.getEscape() : "";
    return Optional.of(
        SearchQuery.newBuilder()
            .setLike(
                SearchQuery.Like.newBuilder()
                    .setField(fieldName)
                    .setPattern(likeFilter.getPattern())
                    .setEscape(escape)
                    .setCaseInsensitive(true))
            .build());
  }
}
