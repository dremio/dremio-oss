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
package com.dremio.datastore;

import static com.dremio.datastore.indexed.IndexKey.LOWER_CASE_SUFFIX;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.exec.proto.SearchProtos;
import com.google.common.base.Preconditions;

/**
 * A collection of helper methods for creating Indexed Search Query
 */
public class IndexedSearchQueryConverterUtil {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IndexedSearchQueryConverterUtil.class);

  public static SearchTypes.SearchQuery toSearchQuery(SearchProtos.SearchQuery searchQuery, Map<String, IndexKey> indexMap) {
    IndexKey key;
    List<SearchTypes.SearchQuery> searchQueryList;
    switch (searchQuery.getQueryCase())
    {
      case EQUALS:
        switch (searchQuery.getEquals().getValueCase()) {
          case INTVALUE:
            key = indexMap.get(searchQuery.getEquals().getField());

            if (key == null) {
              LOGGER.debug("The filter on field {} is not pushed down as it is not indexed", searchQuery.getEquals().getField());
              return null;
            }
            return SearchQueryUtils.newTermQuery(key.getIndexFieldName(),
              searchQuery.getEquals().getIntValue());

          case STRINGVALUE:
            key = indexMap.get(searchQuery.getEquals().getField());

            if (key == null) {
              LOGGER.debug("The filter on field {} is not pushed down as it is not indexed", searchQuery.getEquals().getField());
              return null;
            }

            //If we receive ProtocolMessageEnum label in search request but the index is on its Number value
            if (key.getValueType().equals(Integer.class) && key.getConverter()!=null) {
              return SearchQueryUtils.newTermQuery(key.getIndexFieldName(),
                (Integer) key.getConverter().apply(searchQuery.getEquals().getStringValue()));
            }
            return SearchQueryUtils.newTermQuery(key.getIndexFieldName(),
              searchQuery.getEquals().getStringValue());

          case VALUE_NOT_SET:
          default:
            throw new UnsupportedOperationException(String.format("%s is not supported",
              searchQuery.getEquals().getValueCase()));
        }
      case AND:
        searchQueryList = searchQuery.getAnd()
          .getClausesList()
          .stream()
          .map((query) -> {
              LOGGER.debug("Calling and query {}", query);
              return toSearchQuery(query, indexMap);
          })
          .filter(q -> q != null)
          .collect(Collectors.toList());
        LOGGER.info("After and query filter collect part {}",searchQueryList);

        if (searchQueryList.size() == 0) {
          return null;
        } else{
          return SearchQueryUtils.and(searchQueryList);
        }

      case OR:
        searchQueryList = new ArrayList<>();
        for (SearchProtos.SearchQuery query : searchQuery.getOr().getClausesList()) {
          try {
            SearchTypes.SearchQuery currSearchQuery = toSearchQuery(query, indexMap);
            if (currSearchQuery == null) {
              LOGGER.debug("Skipping creating OR search query as one of the clause is not a push down query, {}", searchQuery);
              return null;
            }
            searchQueryList.add(currSearchQuery);
          } catch (EnumSearchValueNotFoundException ex) {
            LOGGER.debug("Skipping search clause {} as it is invalid", query);
          }
        }
        return searchQueryList.size() != 0 ? SearchQueryUtils.or(searchQueryList) : null;
      case LIKE:
        key = indexMap.get(searchQuery.getLike().getField());
        if (key == null) {
          LOGGER.debug("The filter on field {} is not pushed down as it is not indexed", searchQuery.getLike().getField());
          return null;
        }
        final String escape = searchQuery.getLike().getEscape().isEmpty() ? null :
          searchQuery.getLike().getEscape();
        return getLikeQuery(key.getIndexFieldName(),
          searchQuery.getLike().getPattern(), escape, searchQuery.getLike().getCaseInsensitive());

      case GREATER_THAN:
        key = indexMap.get(searchQuery.getGreaterThan().getField());
        if (key == null) {
          LOGGER.debug("The filter on field {} is not pushed down as it is not indexed",
            searchQuery.getGreaterThan().getField());
          return null;
        }
        return SearchQueryUtils.newRangeLong(key.getIndexFieldName(),
          searchQuery.getGreaterThan().getValue(), null, false, false);

      default:
      case QUERY_NOT_SET:
        throw new UnsupportedOperationException(String.format("%s is not supported",
          searchQuery.getQueryCase()));
    }
  }

  public static SearchTypes.SearchQuery getLikeQuery(
    String fieldName,
    String pattern,
    String escape,
    boolean caseInsensitive
  ){
    Preconditions.checkArgument(escape == null || escape.length() == 1, "An escape must be a single character.");
    StringBuilder sb = new StringBuilder();
    final char e = escape == null ? '\\' : escape.charAt(0);
    boolean escaped = false;

    for (int i = 0; i < pattern.length(); i++){
      char c = pattern.charAt(i);

      if (escaped) {
        sb.append(c);
        escaped = false;
        continue;
      }

      if (c == e) {
        sb.append('\\');
        escaped = true;
        continue;
      }

      switch (c){
        case '%':
          sb.append("*");
          break;

        // ESCAPE * if it occurs
        case '*':
          sb.append("\\*");
          break;

        // ESCAPE ? if it occurs
        case '?':
          sb.append("\\?");
          break;

        default:
          sb.append(c);
          break;
      }
    }
    if (caseInsensitive) {
      return SearchQueryUtils.newWildcardQuery(fieldName + LOWER_CASE_SUFFIX, sb.toString().toLowerCase());
    } else {
      return SearchQueryUtils.newWildcardQuery(fieldName, sb.toString());
    }
  }

}
