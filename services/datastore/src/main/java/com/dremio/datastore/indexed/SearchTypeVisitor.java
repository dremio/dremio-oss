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

import com.dremio.datastore.SearchTypes;

/** Translates {@code SearchQuery}instances into appropriate filters. */
public interface SearchTypeVisitor<R> {

  /** Default visit method that is called by the users of the {@code SearchTypeVisitor<R>} class */
  default R visit(SearchTypes.SearchQuery query) {
    switch (query.getType()) {
      case TERM:
        return visit(query.getTerm());
      case TERM_INT:
        return visit(query.getTermInt());
      case TERM_LONG:
        return visit(query.getTermLong());
      case TERM_DOUBLE:
        return visit(query.getTermDouble());
      case TERM_BOOLEAN:
        return visit(query.getTermBoolean());
      case CONTAINS:
        return visit(query.getContainsText());
      case BOOLEAN:
        return visit(query.getBoolean());
      case MATCH_ALL:
        return visit(query.getMatchAll());
      case NOT:
        return visit(query.getNot());
      case EXISTS:
        return visit(query.getExists(), true);
      case DOES_NOT_EXIST:
        return visit(query.getExists(), false);
      case RANGE_INT:
        return visit(query.getRangeInt());
      case RANGE_LONG:
        return visit(query.getRangeLong());
      case RANGE_DOUBLE:
        return visit(query.getRangeDouble());
      case RANGE_TERM:
        return visit(query.getRangeTerm());
      case PREFIX:
        return visit(query.getPrefix());
      case BOOST:
        return visit(query.getBoost());
      case WILDCARD:
        return visit(query.getWildcard());
        // Since float fields can't be used as index keys, an UnsupportedOperationException
        // will be thrown if the query type is either a RANGE_FLOAT or a TERM_FLOAT.
      case RANGE_FLOAT:
      case TERM_FLOAT:
      default:
        throw new UnsupportedOperationException(
            String.format("Unrecognized query type encountered: %s", query.getType().name()));
    }
  }

  /** Method visited when a {@code Term} is passed */
  R visit(SearchTypes.SearchQuery.Term node);

  /** Method visited when a {@code TermLong} is passed */
  R visit(SearchTypes.SearchQuery.TermLong node);

  /** Method visited when a {@code TermInt} is passed */
  R visit(SearchTypes.SearchQuery.TermInt node);

  /** Method visited when a {@code TermDouble} is passed */
  R visit(SearchTypes.SearchQuery.TermDouble node);

  /** Method visited when a {@code TermBoolean} is passed */
  R visit(SearchTypes.SearchQuery.TermBoolean node);

  /** Method visited when a {@code Contains} is passed */
  R visit(SearchTypes.SearchQuery.Contains node);

  /**
   * Method visited when a {@code Exists} is passed. When the flag {@code doesItExist} is true, the
   * field must exist to satisfy the filter. When the flag is false, the field must not exist to
   * satisfy the filter.
   */
  R visit(SearchTypes.SearchQuery.Exists node, boolean doesItExist);

  /** Method visited when a {@code MatchAll} is passed */
  R visit(SearchTypes.SearchQuery.MatchAll node);

  /** Method visited when a {@code Not} is passed */
  R visit(SearchTypes.SearchQuery.Not node);

  /** Method visited when a {@code Prefix} is passed */
  R visit(SearchTypes.SearchQuery.Prefix node);

  /** Method visited when a {@code RangeTerm} is passed */
  R visit(SearchTypes.SearchQuery.RangeTerm node);

  /** Method visited when a {@code RangeInt} is passed */
  R visit(SearchTypes.SearchQuery.RangeInt node);

  /** Method visited when a {@code RangeDouble} is passed */
  R visit(SearchTypes.SearchQuery.RangeDouble node);

  /** Method visited when a {@code RangeLong} is passed */
  R visit(SearchTypes.SearchQuery.RangeLong node);

  /** Method visited when a {@code Boolean} is passed */
  R visit(SearchTypes.SearchQuery.Boolean node);

  /** Method visited when a {@code Boost} is passed */
  R visit(SearchTypes.SearchQuery.Boost node);

  /** Method visited when a {@code Wildcard} is passed */
  R visit(SearchTypes.SearchQuery.Wildcard node);
}
