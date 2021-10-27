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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.exec.proto.SearchProtos;
import com.google.common.base.Preconditions;
import com.google.protobuf.ProtocolMessageEnum;

/**
 * A collection of helper methods for creating Indexed Search Query
 */
public class IndexedSearchQueryConverterUtil {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IndexedSearchQueryConverterUtil.class);

  public static SearchTypes.SearchQuery toSearchQuery(SearchProtos.SearchQuery searchQuery, Map<String, IndexKey> FIELDS) throws EnumSearchValueNotFoundException {
    IndexKey key;
    List<SearchTypes.SearchQuery> searchQueryList;
    switch (searchQuery.getQueryCase())
    {
      case EQUALS:
        switch (searchQuery.getEquals().getValueCase()) {
          case INTVALUE:
            key = FIELDS.get(searchQuery.getEquals().getField());

            if (key == null) {
              LOGGER.warn("The filter on field {} is not pushed down as it is not indexed", searchQuery.getEquals().getField());
              return null;
            }
            return SearchQueryUtils.newTermQuery(key.getIndexFieldName(),
              searchQuery.getEquals().getIntValue());

          case STRINGVALUE:
            key = FIELDS.get(searchQuery.getEquals().getField());

            if (key == null) {
              LOGGER.warn("The filter on field {} is not pushed down as it is not indexed", searchQuery.getEquals().getField());
              return null;
            }

            //If we receive ProtocolMessageEnum label in search request but the index is on its Number value
            if (key.getValueType().equals(Integer.class) && key.getEnumType()!=null) {
              ProtocolMessageEnum protocolMessageEnum;
              try {
                protocolMessageEnum = (ProtocolMessageEnum) Enum.valueOf(key.getEnumType(), searchQuery.getEquals().getStringValue());
                return SearchQueryUtils.newTermQuery(key.getIndexFieldName(),
                  protocolMessageEnum.getNumber());
              } catch (IllegalArgumentException ex) {
                LOGGER.info("No enum value found corresponding to string {}", searchQuery.getEquals().getStringValue());
                throw new EnumSearchValueNotFoundException("No enum value found corresponding to string "
                  + searchQuery.getEquals().getStringValue() + ", termQuery field : " + searchQuery.getEquals().getField());
              }
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
              LOGGER.info("Calling and query {}", query);
              return toSearchQuery(query, FIELDS);
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
        searchQueryList = searchQuery.getOr()
          .getClausesList()
          .stream()
          .map((query) -> {
            try {
              return toSearchQuery(query, FIELDS);
            } catch (EnumSearchValueNotFoundException ex) {
              //If a query in OR throws exception that query should be ignored
              return null;
            }
          })
          .filter(q -> q != null)
          .collect(Collectors.toList());
        if (searchQueryList.size() == 0) {
          return null;
        } else {
          return SearchQueryUtils.or(searchQueryList);
        }
      case LIKE:
        key = FIELDS.get(searchQuery.getLike().getField());
        if (key == null) {
          LOGGER.warn("The filter on field {} is not pushed down as it is not indexed", searchQuery.getLike().getField());
          return null;
        }
        final String escape = searchQuery.getLike().getEscape().isEmpty() ? null :
          searchQuery.getLike().getEscape();
        return getLikeQuery(key.getIndexFieldName(),
          searchQuery.getLike().getPattern(), escape, searchQuery.getLike().getCaseInsensitive());
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
