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
package com.dremio.datastore.indexed;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SearchQuery.MatchAll;
import com.dremio.datastore.SearchTypes.SearchQuery.Not;
import com.dremio.datastore.SearchTypes.SearchQuery.RangeDouble;
import com.dremio.datastore.SearchTypes.SearchQuery.RangeFloat;
import com.dremio.datastore.SearchTypes.SearchQuery.RangeInt;
import com.dremio.datastore.SearchTypes.SearchQuery.RangeLong;
import com.dremio.datastore.SearchTypes.SearchQuery.RangeTerm;
import com.google.common.base.CharMatcher;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;

/**
 * Helper class to convert from a KVStore {@code com.dremio.datastore.SearchQuery} to a Lucene
 * {@code org.apache.lucene.search.Query}
 */
public class LuceneQueryConverter {
  public static final LuceneQueryConverter INSTANCE = new LuceneQueryConverter();
  private final CharMatcher specialCharactersMatcher =
      CharMatcher.anyOf(
              new String(
                  new char[] {
                    WildcardQuery.WILDCARD_ESCAPE,
                    WildcardQuery.WILDCARD_CHAR,
                    WildcardQuery.WILDCARD_STRING
                  }))
          .precomputed();

  public LuceneQueryConverter() {}

  Query toLuceneQuery(SearchQuery query) {
    switch (query.getType()) {
      case BOOLEAN:
        return toBooleanQuery(query.getBoolean());

      case MATCH_ALL:
        return toMatchAllQuery(query.getMatchAll());

      case NOT:
        return toNotQuery(query.getNot());

      case RANGE_DOUBLE:
        return toRangeQuery(query.getRangeDouble());

      case RANGE_FLOAT:
        return toRangeQuery(query.getRangeFloat());

      case RANGE_INT:
        return toRangeQuery(query.getRangeInt());

      case RANGE_LONG:
        return toRangeQuery(query.getRangeLong());

      case RANGE_TERM:
        return toRangeQuery(query.getRangeTerm());

      case TERM:
        return toTermQuery(query.getTerm());

      case WILDCARD:
        return toWildcardQuery(query.getWildcard());

      case TERM_INT:
        return toTermIntQuery(query.getTermInt());

      case TERM_LONG:
        return toTermLongQuery(query.getTermLong());

      case TERM_FLOAT:
        return toTermFloatQuery(query.getTermFloat());

      case TERM_DOUBLE:
        return toTermDoubleQuery(query.getTermDouble());

      case TERM_BOOLEAN:
        return toTermBooleanQuery(query.getTermBoolean());

      case EXISTS:
        return toExistsquery(query.getExists());

      case DOES_NOT_EXIST:
        return toDoesNotExistQuery(query.getExists());

      case BOOST:
        return toBoostQuery(query.getBoost());

      case CONTAINS:
        return toContainsTermQuery(query.getContainsText());

      case PREFIX:
        return toPrefixQuery(query.getPrefix());

      default:
        throw new AssertionError("Unknown query type: " + query);
    }
  }

  private StringBuilder escapeTextForWildcard(String text) {
    final StringBuilder sb = new StringBuilder(text.length());

    for (int i = 0; i < text.length(); i++) {
      char currentChar = text.charAt(i);
      if (specialCharactersMatcher.matches(currentChar)) {
        sb.append(WildcardQuery.WILDCARD_ESCAPE);
      }
      sb.append(currentChar);
    }

    return sb;
  }

  private Query toBooleanQuery(SearchQuery.Boolean booleanQuery) {
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    final BooleanClause.Occur occur;
    switch (booleanQuery.getOp()) {
      case AND:
        occur = BooleanClause.Occur.MUST;
        break;
      case OR:
        occur = BooleanClause.Occur.SHOULD;
        break;
      default:
        throw new AssertionError("Unknown boolean operator: " + booleanQuery.getOp());
    }

    for (SearchQuery clause : booleanQuery.getClausesList()) {
      builder.add(toLuceneQuery(clause), occur);
    }
    return builder.build();
  }

  private Query toMatchAllQuery(MatchAll matchAll) {
    return new MatchAllDocsQuery();
  }

  private Query toNotQuery(Not not) {
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(toLuceneQuery(not.getClause()), BooleanClause.Occur.MUST_NOT);
    return builder.build();
  }

  private Query toRangeQuery(RangeDouble range) {
    return DoublePoint.newRangeQuery(
        range.getField(),
        range.hasMin()
            ? range.getMinInclusive() ? range.getMin() : Math.nextUp(range.getMin())
            : Double.NEGATIVE_INFINITY,
        range.hasMax()
            ? range.getMaxInclusive()
                ? range.getMax()
                : Math.nextAfter(range.getMax(), -Double.MAX_VALUE)
            : Double.POSITIVE_INFINITY);
  }

  private Query toRangeQuery(RangeFloat range) {
    return FloatPoint.newRangeQuery(
        range.getField(),
        range.hasMin()
            ? range.getMinInclusive() ? range.getMin() : Math.nextUp(range.getMin())
            : Float.NEGATIVE_INFINITY,
        range.hasMax()
            ? range.getMaxInclusive()
                ? range.getMax()
                : Math.nextAfter(range.getMax(), -Double.MAX_VALUE)
            : Float.POSITIVE_INFINITY);
  }

  private Query toRangeQuery(RangeInt range) {
    return IntPoint.newRangeQuery(
        range.getField(),
        range.hasMin()
            ? range.getMinInclusive() ? range.getMin() : (range.getMin() + 1)
            : -Integer.MAX_VALUE,
        range.hasMax()
            ? range.getMaxInclusive() ? range.getMax() : (range.getMax() - 1)
            : Integer.MAX_VALUE);
  }

  private Query toRangeQuery(RangeLong range) {
    return LongPoint.newRangeQuery(
        range.getField(),
        range.hasMin()
            ? range.getMinInclusive() ? range.getMin() : (range.getMin() + 1L)
            : -Long.MAX_VALUE,
        range.hasMax()
            ? range.getMaxInclusive() ? range.getMax() : (range.getMax() - 1L)
            : Long.MAX_VALUE);
  }

  private Query toRangeQuery(RangeTerm range) {
    return TermRangeQuery.newStringRange(
        range.getField(),
        range.hasMin() ? range.getMin() : null,
        range.hasMax() ? range.getMax() : null,
        range.getMinInclusive(),
        range.getMaxInclusive());
  }

  private Query toTermQuery(SearchQuery.Term term) {
    return new TermQuery(new Term(term.getField(), term.getValue()));
  }

  private Query toWildcardQuery(SearchQuery.Wildcard wildcard) {
    return new WildcardQuery(new Term(wildcard.getField(), wildcard.getValue()));
  }

  private Query toPrefixQuery(SearchQuery.Prefix prefix) {
    return new PrefixQuery(new Term(prefix.getField(), prefix.getValue()));
  }

  private Query toContainsTermQuery(SearchQuery.Contains containsQuery) {
    final StringBuilder sb = escapeTextForWildcard(containsQuery.getValue());
    sb.insert(0, WildcardQuery.WILDCARD_STRING).append(WildcardQuery.WILDCARD_STRING);
    return new WildcardQuery(new Term(containsQuery.getField(), sb.toString()));
  }

  private Query toTermIntQuery(SearchQuery.TermInt term) {
    return IntPoint.newRangeQuery(term.getField(), term.getValue(), term.getValue());
  }

  private Query toTermLongQuery(SearchQuery.TermLong term) {
    return LongPoint.newRangeQuery(term.getField(), term.getValue(), term.getValue());
  }

  private Query toTermFloatQuery(SearchQuery.TermFloat term) {
    return FloatPoint.newRangeQuery(term.getField(), term.getValue(), term.getValue());
  }

  private Query toTermDoubleQuery(SearchQuery.TermDouble term) {
    return DoublePoint.newRangeQuery(term.getField(), term.getValue(), term.getValue());
  }

  private Query toTermBooleanQuery(SearchQuery.TermBoolean term) {
    // there is no BooleanPoint or any other structure that can carry boolean query in lucene
    // support for it will be handled in DX-60829
    throw new UnsupportedOperationException(
        "The TermBoolean is not supported for "
            + LuceneQueryConverter.class.getName()
            + ". For more info, see: DX-60829.");
  }

  private Query toExistsquery(SearchQuery.Exists exists) {
    return new DocValuesFieldExistsQuery(exists.getField());
  }

  private Query toDoesNotExistQuery(SearchQuery.Exists exists) {
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    builder.add(new DocValuesFieldExistsQuery(exists.getField()), BooleanClause.Occur.MUST_NOT);
    return builder.build();
  }

  private Query toBoostQuery(SearchQuery.Boost boost) {
    return new BoostQuery(toLuceneQuery(boost.getClause()), boost.getBoost());
  }
}
