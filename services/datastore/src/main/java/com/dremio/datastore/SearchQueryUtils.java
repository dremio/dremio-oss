/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.datastore;

import java.util.Arrays;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.indexed.IndexKey;

/**
 * A collection of helper methods to build {@code com.dremio.datastore.SearchTypes.SearchQuery}
 * expressions.
 */
public final class SearchQueryUtils {

  private SearchQueryUtils() { }

  /**
   * Create a new match-all query
   * @return a query instance
   */
  public static final SearchQuery newMatchAllQuery() {
    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.MATCH_ALL)
        .setMatchAll(SearchQuery.MatchAll.newBuilder())
        .build();
  }

  /**
   * Create a term query
   *
   * @param field the field to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(String field, String value) {
    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.TERM)
        .setTerm(SearchQuery.Term.newBuilder().setField(field).setValue(value))
        .build();
  }

  /**
   * Create a term query
   *
   * @param indexKey the index key to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code indexKey} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(IndexKey indexKey, String value) {
    return newTermQuery(indexKey.getIndexFieldName(), value);
  }

  /**
   * Create a term query
   *
   * @param field the field to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(String field, int value) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.TERM_INT)
      .setTermInt(SearchQuery.TermInt.newBuilder().setField(field).setValue(value))
      .build();
  }

  /**
   * Create a term query
   *
   * @param indexKey the index key to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code indexKey} is {@code null}
   */
  public static final SearchQuery newTermQuery(IndexKey indexKey, int value) {
    return newTermQuery(indexKey.getIndexFieldName(), value);
  }

  /**
   * Create a term query
   *
   * @param field the field to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(String field, long value) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.TERM_LONG)
      .setTermLong(SearchQuery.TermLong.newBuilder().setField(field).setValue(value))
      .build();
  }

  /**
   * Create a term query
   *
   * @param indexKey the index key to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code indexKey} is {@code null}
   */
  public static final SearchQuery newTermQuery(IndexKey indexKey, long value) {
    return newTermQuery(indexKey.getIndexFieldName(), value);
  }

  /**
   * Create a term query
   *
   * @param field the field to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(String field, float value) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.TERM_FLOAT)
      .setTermFloat(SearchQuery.TermFloat.newBuilder().setField(field).setValue(value))
      .build();
  }

  /**
   * Create a term query
   *
   * @param indexKey the index key to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code indexKey} is {@code null}
   */
  public static final SearchQuery newTermQuery(IndexKey indexKey, float value) {
    return newTermQuery(indexKey.getIndexFieldName(), value);
  }

  /**
   * Create a term query
   *
   * @param field the field to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newTermQuery(String field, double value) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.TERM_DOUBLE)
      .setTermDouble(SearchQuery.TermDouble.newBuilder().setField(field).setValue(value))
      .build();
  }

  /**
   * Create a term query
   *
   * @param indexKey the index key to scan
   * @param value the value to look for
   * @return a query instance
   * @throws NullPointerException if {@code indexKey} is {@code null}
   */
  public static final SearchQuery newTermQuery(IndexKey indexKey, double value) {
    return newTermQuery(indexKey.getIndexFieldName(), value);
  }

  /**
   * Create a query that returns documents where given field has one or mode values defined.
   * @param field
   * @return
   */
  public static final SearchQuery newExistsQuery(String field) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.EXISTS)
      .setExists(SearchQuery.Exists.newBuilder().setField(field))
      .build();
  }


  /**
   * Create a query that returns documents where given field is not defined.
   * @param field
   * @return
   */
  public static final SearchQuery newDoesNotExistQuery(String field) {
    return SearchQuery.newBuilder()
      .setType(SearchQuery.Type.DOES_NOT_EXIST)
      .setExists(SearchQuery.Exists.newBuilder().setField(field))
      .build();
  }

  /**
   * Create a wildcard query
   *
   * @param field the field to scan
   * @param value the wildcard expression to look for
   * @return a query instance
   * @throws NullPointerException if {@code field} or {@code value} is {@code null}
   */
  public static final SearchQuery newWildcardQuery(String field, String value) {
    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.WILDCARD)
        .setWildcard(SearchQuery.Wildcard.newBuilder().setField(field).setValue(value))
        .build();
  }

  /**
   * Create a range query based on int values
   *
   * @param field the field to scan
   * @param min the minimum value, or {@code null} if unbounded
   * @param max the maximum value, or {@code null} if unbounded
   * @param minInclusive if true, min value is included into results
   * @param maxInclusive if true, max value is included into results
   * @return a query instance
   * @throws NullPointerException if {@code field} is {@code null}
   */
  public static final SearchQuery newRangeInt(String field, Integer min, Integer max, boolean minInclusive,
      boolean maxInclusive) {
    final SearchQuery.RangeInt.Builder builder =  SearchQuery.RangeInt.newBuilder()
        .setField(field)
        .setMinInclusive(minInclusive)
        .setMaxInclusive(maxInclusive);

    if (min != null) {
      builder.setMin(min);
    }
    if (max != null) {
      builder.setMax(max);
    }

    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.RANGE_INT)
        .setRangeInt(builder)
        .build();
  }

  /**
   * Create a range query based on long values
   *
   * @param field the field to scan
   * @param min the minimum value, or {@code null} if unbounded
   * @param max the maximum value, or {@code null} if unbounded
   * @param minInclusive if true, min value is included into results
   * @param maxInclusive if true, max value is included into results
   * @return a query instance
   * @throws NullPointerException if {@code field} is {@code null}
   */
  public static final SearchQuery newRangeLong(String field, Long min, Long max, boolean minInclusive,
      boolean maxInclusive) {
    final SearchQuery.RangeLong.Builder builder =  SearchQuery.RangeLong.newBuilder()
        .setField(field)
        .setMinInclusive(minInclusive)
        .setMaxInclusive(maxInclusive);

    if (min != null) {
      builder.setMin(min);
    }
    if (max != null) {
      builder.setMax(max);
    }

    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.RANGE_LONG)
        .setRangeLong(builder)
        .build();
  }

  /**
   * Create a range query based on float values
   *
   * @param field the field to scan
   * @param min the minimum value, or {@code null} if unbounded
   * @param max the maximum value, or {@code null} if unbounded
   * @param minInclusive if true, min value is included into results
   * @param maxInclusive if true, max value is included into results
   * @return a query instance
   * @throws NullPointerException if {@code field} is {@code null}
   */
  public static final SearchQuery newRangeFloat(String field, Float min, Float max, boolean minInclusive,
      boolean maxInclusive) {
    final SearchQuery.RangeFloat.Builder builder =  SearchQuery.RangeFloat.newBuilder()
        .setField(field)
        .setMinInclusive(minInclusive)
        .setMaxInclusive(maxInclusive);

    if (min != null) {
      builder.setMin(min);
    }
    if (max != null) {
      builder.setMax(max);
    }

    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.RANGE_FLOAT)
        .setRangeFloat(builder)
        .build();
  }

  /**
   * Create a range query based on double values
   *
   * @param field the field to scan
   * @param min the minimum value, or {@code null} if unbounded
   * @param max the maximum value, or {@code null} if unbounded
   * @param minInclusive if true, min value is included into results
   * @param maxInclusive if true, max value is included into results
   * @return a query instance
   * @throws NullPointerException if {@code field} is {@code null}
   */
  public static final SearchQuery newRangeDouble(String field, Double min, Double max, boolean minInclusive,
      boolean maxInclusive) {
    final SearchQuery.RangeDouble.Builder builder =  SearchQuery.RangeDouble.newBuilder()
        .setField(field)
        .setMinInclusive(minInclusive)
        .setMaxInclusive(maxInclusive);

    if (min != null) {
      builder.setMin(min);
    }
    if (max != null) {
      builder.setMax(max);
    }

    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.RANGE_DOUBLE)
        .setRangeDouble(builder)
        .build();
  }

  /**
   * Create a range query based on string values
   *
   * @param field the field to scan
   * @param min the minimum value, or {@code null} if unbounded
   * @param max the maximum value, or {@code null} if unbounded
   * @param minInclusive if true, min value is included into results
   * @param maxInclusive if true, max value is included into results
   * @return a query instance
   * @throws NullPointerException if {@code field} is {@code null}
   */
  public static final SearchQuery newRangeTerm(String field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    final SearchQuery.RangeTerm.Builder builder =  SearchQuery.RangeTerm.newBuilder()
        .setField(field)
        .setMinInclusive(minInclusive)
        .setMaxInclusive(maxInclusive);

    if (min != null) {
      builder.setMin(min);
    }
    if (max != null) {
      builder.setMax(max);
    }

    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.RANGE_TERM)
        .setRangeTerm(builder)
        .build();
  }

  /**
   * Create a query inversing another query
   *
   * @param query the query to return the negation of
   * @return a query instance
   * @throws NullPointerException if {@code query} is {@code null}
   */
  public static final SearchQuery not(SearchQuery query) {
    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.NOT)
        .setNot(SearchQuery.Not.newBuilder().setClause(query))
        .build();
  }

  /**
   * Create a query matching at least one of the sub queries
   *
   * @param queries the sub queries
   * @return a query instance
   * @throws NullPointerException if {@code queries} or any of the subqueries is {@code null}
   */
  public static final SearchQuery or(SearchQuery... queries) {
    return or(Arrays.asList(queries));
  }

  /**
   * Create a query matching at least one of the sub queries
   *
   * @param queries the sub queries
   * @return a query instance
   * @throws NullPointerException if {@code queries} or any of the subqueries is {@code null}
   */
  public static final SearchQuery or(Iterable<SearchQuery> queries) {
    return newBooleanQuery(SearchQuery.BooleanOp.OR, queries);
  }

  /**
   * Create a query matching all of of the sub queries
   *
   * @param queries the sub queries
   * @return a query instance
   * @throws NullPointerException if {@code queries} or any of the subqueries is {@code null}
   */
  public static final SearchQuery and(SearchQuery... queries) {
    return and(Arrays.asList(queries));
  }

  /**
   * Create a query matching all of of the sub queries
   *
   * @param queries the sub queries
   * @return a query instance
   * @throws NullPointerException if {@code queries} or any of the subqueries is {@code null}
   */
  public static final SearchQuery and(Iterable<SearchQuery> queries) {
    return newBooleanQuery(SearchQuery.BooleanOp.AND, queries);
  }


  /**
   * Create a query matching sub queries according to a provided operator
   *
   * @param op the boolean operator
   * @param queries the sub queries
   * @return a query instance
   * @throws NullPointerException if {@code op}, {@code queries} or any of the subqueries is {@code null}
   */
  private static final SearchQuery newBooleanQuery(SearchQuery.BooleanOp op, Iterable<SearchQuery> queries) {
    return SearchQuery.newBuilder()
        .setType(SearchQuery.Type.BOOLEAN)
        .setBoolean(
            SearchQuery.Boolean.newBuilder()
            .setOp(op)
            .addAllClauses(queries))
        .build();
  }
}
