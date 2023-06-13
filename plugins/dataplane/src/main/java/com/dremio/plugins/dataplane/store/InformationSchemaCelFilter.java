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

import java.util.ArrayList;
import java.util.List;

import com.dremio.service.catalog.SearchQuery;
import com.google.common.base.Preconditions;

/**
 * This class converts SearchQuery into INFORMATION_SCHEMA SPECIFIC CEL filter that is being used by nessie
 * if searchQuery has the structure something like :
 * Where table_name = "table" and table_schema like '%nessie.folder%"
 * then it will return "(entry.name == "table") && ((name + entry.namespace + entry.name).matches(^.*nessie_folder,*$))"
 */
public final class InformationSchemaCelFilter {

  private InformationSchemaCelFilter() {}

  private static final String SEARCH_NAME = "SEARCH_NAME";
  private static final String SEARCH_SCHEMA = "SEARCH_SCHEMA";

  private static String getEqualSearchSchema(boolean isSchemata, String equalsPattern, String path, String sourceName) {
    if (splitPath(equalsPattern).length == 1) {
      if (isSchemata) {
        return null;
      }
      return String.format(
        "entry.namespace == '' && (%s) == (%s)",
        SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(sourceName),
        SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(equalsPattern));
    }
    return String.format(
      "(%s) == (%s)",
      path,
      SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(equalsPattern));
  }

  private static String getLikeSearchSchema(boolean isSchemata, String regexPattern, String sourceName) {
    String regexPatternAsRawCelStringLiteral = SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(regexPattern);
    String sourceNameAsCelRawStringLiteral = SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(sourceName);
    String nameElements = sourceNameAsCelRawStringLiteral + " + '.' + entry.namespace + '.' + entry.name";
    String namespace = sourceNameAsCelRawStringLiteral + " + '.' + entry.namespace";
    String nameOnly = sourceNameAsCelRawStringLiteral + " + '.' + entry.name";
    if (isSchemata) {
      return String.format(
        "(%s).matches(%s) || (%s).matches(%s) || (%s).matches(%s)",
        sourceNameAsCelRawStringLiteral,
        regexPatternAsRawCelStringLiteral,
        nameElements,
        regexPatternAsRawCelStringLiteral,
        nameOnly,
        regexPatternAsRawCelStringLiteral);
    }
    return String.format(
      "(%s).matches(%s) || (%s).matches(%s)",
      sourceNameAsCelRawStringLiteral,
      regexPatternAsRawCelStringLiteral,
      namespace,
      regexPatternAsRawCelStringLiteral);
  }

  /**
   * @param searchQuery searchQuery
   * @param isSchemata represents if it is being called by information_schema.SCHEMATA
   * @return CEL filter of the searchQuery (leaf case)
   */
  private static String leafSearchQueryToCelFiler(SearchQuery searchQuery, boolean isSchemata, String sourceName) {
    String fieldName, path;
    switch (searchQuery.getQueryCase()) {
      case EQUALS:
        String equalsPattern = searchQuery.getEquals().getStringValue();
        fieldName = searchQuery.getEquals().getField();
        path = resolvePath(equalsPattern, sourceName, isSchemata);
        if (fieldName.equals(SEARCH_NAME)) {
          return String.format("(%s) == entry.name", SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(equalsPattern));
        } else if (fieldName.equals(SEARCH_SCHEMA)) {
          return getEqualSearchSchema(isSchemata, equalsPattern, path, sourceName);
        } else {
          throw new IllegalStateException(String.format("Field should be SEARCH_NAME or SEARCH_SCHEMA. Provided = %s", fieldName));
        }
      case LIKE:
        String regexPattern = SearchQueryToCelConversionUtilities.likeQueryToRe2Regex(searchQuery.getLike());
        fieldName = searchQuery.getLike().getField();
        path = resolvePath(regexPattern, sourceName, isSchemata);
        if (fieldName.equals(SEARCH_NAME)) {
          return String.format("entry.name.matches(%s)", SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(regexPattern));
        } else if (fieldName.equals(SEARCH_SCHEMA)) {
          if (!containsWildCard(searchQuery.getLike())) { // you don't have wildcard, it is same as equal
            return getEqualSearchSchema(isSchemata, searchQuery.getLike().getPattern(), path, sourceName);
          } else {
            return getLikeSearchSchema(isSchemata, regexPattern, sourceName);
          }
        } else {
          throw new IllegalStateException(String.format("Field should be SEARCH_NAME or SEARCH_SCHEMA. Provided = %s",fieldName));
        }
      default:
        throw new IllegalStateException(String.format("SearchQuery should be EQUALS or LIKE. Provided = %s", searchQuery.getQueryCase()));
    }
  }

  /**
   * @param searchQuery searchQuery
   * @param includeTableNameInQuery  represents if it is being called by information_schema.SCHEMATA
   * @return CEL filter of the searchQueries
   */
  public static String getInformationSchemaFilter(SearchQuery searchQuery, boolean includeTableNameInQuery, String sourceName) {
    if (searchQuery == null) {
      return null;
    }
    // check the name of the source equals to the source name.
    List<SearchQuery> searchQueries;
    String delimiter;
    switch (searchQuery.getQueryCase()) {
      case EQUALS:
        //Intentional fallthrough
      case LIKE:
        return leafSearchQueryToCelFiler(searchQuery, includeTableNameInQuery, sourceName);
      case AND:
        searchQueries = searchQuery.getAnd().getClausesList();
        delimiter = " && ";
        break;
      case OR:
        searchQueries = searchQuery.getOr().getClausesList();
        delimiter = " || ";
        break;
      default:
        throw new IllegalStateException(String.format("SearchQuery should be one of the following to get celFilter: EQUALS, LIKE, AND, OR. Provided = %s", searchQuery.getQueryCase()));
    }

    List<String> res = new ArrayList<>();
    for (SearchQuery s : searchQueries) {
      String subFilter = getInformationSchemaFilter(s, includeTableNameInQuery, sourceName);
      if (subFilter != null) {
        subFilter = "(" + subFilter + ")";
        res.add(subFilter);
      }
    }

    return res.isEmpty() ? null : "(" + String.join(delimiter, res) + ")";
  }

  /**
   * @return checking if we need to add entry.namespace in our cel filter. In schemata, we should include entry.namespace in our celFilter
   * if and only if we have three elements in our value. for example, if we have path that is source.folder1.folder2,
   * which has three elements, the celFilter should be source.<entry.namespace><entry.name>,
   * while if we have path that is source.folder1, which has two elements, the celFilter should be source.folder1, which is source.<entry.name>
   */
  private static Boolean shouldIncludeNamespace(String value) {
    String[] path = splitPath(value);
    return path.length > 2;
  }

  /**
   * @return split values on . that is followed by an even number of double quotes
   */
  private static String[] splitPath(String value) {
    return value.split("\\.(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
  }

  /**
   * TODO: DX-58997 - Clarify naming and usage
   * resolvePath method is the method that helps you to generate celFilter  for EQUAL Query ONLY.
   * There are several cases we should handle:
   * if we are not looking for a source and if we need to include name of the entry,
   */
  private static String resolvePath(String value, String sourceName, boolean includeEntryName) {
    StringBuilder path = new StringBuilder(SearchQueryToCelConversionUtilities.convertToRawCelStringLiteral(sourceName));
    if (splitPath(value).length != 1) {
      if (includeEntryName) {
        if (shouldIncludeNamespace(value)) {
          path.append(" + '.' + entry.namespace + '.' + entry.name");
        } else {
          path.append(" + '.' + entry.name");
        }
      } else {
        path.append(" + '.' + entry.namespace");
      }
    }
    return path.toString();
  }

  private static boolean containsWildCard(SearchQuery.Like likeQuery) {
    String pattern = likeQuery.getPattern();
    String escape = likeQuery.getEscape();
    Preconditions.checkArgument("".equals(escape) || escape.length() == 1, "An escape must be a single character.");
    final char e = "".equals(escape) ? '\\' : escape.charAt(0);
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        escaped = false;
        continue;
      }

      if (c == e) {
        escaped = true;
        continue;
      }

      switch (c) {
        case '%':
          return true;

        default:
          break;
      }
    }
    return false;
  }
}
