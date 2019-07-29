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

import java.util.List;
import java.util.regex.Pattern;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.store.ischema.ExpressionConverter;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Helper class to create filter and predicates for metadata queries
 */
public final class MetadataProviderConditions {

  public static final Predicate<String> ALWAYS_TRUE = Predicates.alwaysTrue();

  private static final String SQL_LIKE_ANY_STRING_PATTERN = "%";

  private MetadataProviderConditions() {
  }

  public static Predicate<String> getTableTypePredicate(List<String> tableTypeFilter){
    if (tableTypeFilter.isEmpty()) {
      return ALWAYS_TRUE;
    }

    final ImmutableSet<String> strings = ImmutableSet.copyOf(tableTypeFilter);
    return new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return strings.contains(input);
      }};
  }

  public static Predicate<String> getLikePredicate(LikeFilter filter){
    if(filter == null || !filter.hasPattern()) {
      return ALWAYS_TRUE;
    }

    if (SQL_LIKE_ANY_STRING_PATTERN.equals(filter.getPattern())) {
      return ALWAYS_TRUE;
    }

    final String patternString = RegexpUtil.sqlToRegexLike(filter.getPattern(), filter.hasEscape() ? filter.getEscape().charAt(0) : (char) 0);
    final Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
    return input -> pattern.matcher(input).matches();
  }

  /**
   * Helper method to create a {@link SearchQuery} that combines the given filters with an AND.
   * @param schemaNameFilter Optional filter on <code>schema name</code>
   * @param tableNameFilter Optional filter on <code>table name</code>
   * @return
   */
  public static SearchQuery createFilter(
      final LikeFilter schemaNameFilter,
      final LikeFilter tableNameFilter) {

    return combineFunctions(
        OpType.AND,
        createLikeFunctionExprNode(DatasetIndexKeys.UNQUOTED_SCHEMA.getIndexFieldName(), schemaNameFilter),
        createLikeFunctionExprNode(DatasetIndexKeys.UNQUOTED_NAME.getIndexFieldName(), tableNameFilter)
        );
  }

  /**
   * Helper method to create {@link SearchQuery} from {@link LikeFilter}.
   * @param fieldName Name of the field on which the like expression is applied.
   * @param likeFilter
   * @return {@link SearchQuery} for given arguments. Null if the <code>likeFilter</code> is null.
   */
  public static SearchQuery createLikeFunctionExprNode(String fieldName, LikeFilter likeFilter) {
    if (likeFilter == null || !likeFilter.hasPattern()) {
      return null;
    }

    String pattern = likeFilter.getPattern();
    if (SQL_LIKE_ANY_STRING_PATTERN.equals(pattern)) {
      return null;
    }
    String escape = likeFilter.hasEscape() ? likeFilter.getEscape() : null;
    return ExpressionConverter.getLikeQuery(fieldName, pattern, escape, true);
  }

  /**
   * Helper method to create {@link SearchQuery} from {@code List<String>}.
   * @param fieldName Name of the filed on which the like expression is applied.
   * @param valuesFilter a list of values
   * @return {@link SearchQuery} for given arguments. Null if the <code>valuesFilter</code> is null.
   */
  public static SearchQuery createInFunctionExprNode(final String fieldName, List<String> valuesFilter) {
    if (valuesFilter == null) {
      return null;
    }

    return SearchQueryUtils.or(FluentIterable.from(valuesFilter).transform(new Function<String, SearchQuery>(){
      @Override
      public SearchQuery apply(String input) {
        return SearchQueryUtils.newTermQuery(fieldName, input);
      }}));

  }

  private static enum OpType {
    AND, OR
  }
  /** Helper method to combine two {@link SearchQuery}s with a given <code>functionName</code>. If one of them is
   * null, other one is returned as it is.
   */
  private static SearchQuery combineFunctions(
      final OpType opType,
      final SearchQuery q1,
      final SearchQuery q2) {
    if (q1 == null) {
      return q2;
    }

    if (q2 == null) {
      return q1;
    }

    switch(opType) {
    case AND:
      return SearchQueryUtils.and(ImmutableList.of(q1, q2));
    case OR:
      return SearchQueryUtils.or(ImmutableList.of(q1, q2));
    }

    throw new UnsupportedOperationException(opType.toString());
  }
}
