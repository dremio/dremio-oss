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
package com.dremio.datastore.indexed;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.NoArgRSQLVisitorAdapter;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLOperators;

/**
 * Transform RQL queries into indexed store queries.
 * RQL Syntax:
 * EQUALS =>  =, =eq=
 * NOT_EQUALS => !=, =ne=
 * OR => |
 * AND => &
 * LESS_THAN => =lt=
 * LESS_THAN_EQUALS => =le=
 * GREATER_THAN => =gt=
 * GREATER_THAN_EQUALS => =ge=
 * CONTAINS => =contains=
 *
 * TODO: support analyzer using SearchQueryBuilder
 */
public final class SearchFilterToQueryConverter {

  private static final ComparisonOperator CONTAINS = new ComparisonOperator("=contains=", false);
  private static Set<ComparisonOperator> RSQL_OPERATORS = RSQLOperators.defaultOperators();

  static {
    RSQL_OPERATORS.add(CONTAINS);
  }

  private final FilterIndexMapping mapping;
  private final String visitorName = "visitor";
  private final Visitor visitor = new Visitor();

  public static SearchQuery toQuery(String filterStr, FilterIndexMapping mapping){

    if(Strings.isNullOrEmpty(filterStr)){
      return SearchQueryUtils.newMatchAllQuery();
    }

    final Node root = new RSQLParser(RSQL_OPERATORS).parse(filterStr);
    return root.accept(new SearchFilterToQueryConverter(mapping).visitor);
  }

  private SearchFilterToQueryConverter(FilterIndexMapping mapping) {
    this.mapping = mapping;
  }

  private final class Visitor extends NoArgRSQLVisitorAdapter<SearchQuery> {
    @Override
    public SearchQuery visit(final AndNode node) {
      final List<SearchQuery> queries = new ArrayList<>();
      for (final Node child: node.getChildren()) {
        queries.add(child.accept(this));
      }
      return SearchQueryUtils.and(queries);
    }

    @Override
    public SearchQuery visit(final OrNode node) {
      final List<SearchQuery> queries = new ArrayList<>();
      for (final Node child: node.getChildren()) {
        queries.add(child.accept(this));
      }
      return SearchQueryUtils.or(queries);
    }

    @Override
    public SearchQuery visit(final ComparisonNode node) {
      return buildLeafNodeQuery(node);
    }

  }

  private SearchQuery buildLeafNodeQuery(final ComparisonNode node) throws IllegalArgumentException {
    final IndexKey indexKey = mapping.getKey(node.getSelector());
    final String name = indexKey != null ? indexKey.getIndexFieldName() : null;
    final Class<?> clazz = indexKey != null ? indexKey.getValueType() : null;
    final List<Object> processedArgs = validate(node, name, clazz);

    if (node.getOperator().equals(RSQLOperators.EQUAL)) {
      return createEqualsQuery(clazz, name, processedArgs.get(0),indexKey.getReservedValues());
    } else if (node.getOperator().equals(RSQLOperators.NOT_EQUAL)) {
      return SearchQueryUtils.not(createEqualsQuery(clazz, name, processedArgs.get(0), null));
    } else if (node.getOperator().equals(RSQLOperators.LESS_THAN)) {
      return createRangeQuery(clazz, name, processedArgs.get(0), RSQLOperators.LESS_THAN);
    } else if (node.getOperator().equals(RSQLOperators.LESS_THAN_OR_EQUAL)) {
      return createRangeQuery(clazz, name, processedArgs.get(0), RSQLOperators.LESS_THAN_OR_EQUAL);
    } else if (node.getOperator().equals(RSQLOperators.GREATER_THAN)) {
      return createRangeQuery(clazz, name, processedArgs.get(0), RSQLOperators.GREATER_THAN);
    } else if (node.getOperator().equals(RSQLOperators.GREATER_THAN_OR_EQUAL)) {
      return createRangeQuery(clazz, name, processedArgs.get(0), RSQLOperators.GREATER_THAN_OR_EQUAL);
    } else if (node.getOperator().equals(RSQLOperators.IN)) {
      throw new IllegalArgumentException("IN operator not yet supported");
    } else if (node.getOperator().equals(RSQLOperators.NOT_IN)) {
      throw new IllegalArgumentException("NOT_IN operator not yet supported");
    } else if (node.getOperator().equals(CONTAINS)) {
      return createMatchAllFieldsQuery((String)processedArgs.get(0), mapping);
    } else {
      throw new IllegalArgumentException(format("%s: Invalid search operator %s", visitorName, node.toString()));
    }
  }

  // TODO case insensitive search?
  private SearchQuery createEqualsQuery(final Class<?> cls, final String name, final Object value, final Map<String, SearchQuery> reservedValues) {
    if (cls.equals(String.class)) {
      final String strValue = (String)value;
      if (reservedValues != null && reservedValues.containsKey(strValue)) {
        return reservedValues.get(strValue);
      }
      if (!strValue.contains("*")) {
        return SearchQueryUtils.newTermQuery(name, strValue);
      } else {
        return SearchQueryUtils.newWildcardQuery(name, strValue);
      }
    } else if (cls.equals(Boolean.class)) {
      return createBooleanFieldQuery(name, value);
    } else {
      return createRangeQuery(cls, name, value, RSQLOperators.EQUAL);
    }
  }

  private SearchQuery createRangeQuery(Class<?> cls, String name, Object value, ComparisonOperator type) {

    boolean minInclusive = type == RSQLOperators.GREATER_THAN_OR_EQUAL || type == RSQLOperators.EQUAL;
    boolean maxInclusive = type == RSQLOperators.LESS_THAN_OR_EQUAL || type == RSQLOperators.EQUAL;

    if (Number.class.isAssignableFrom(cls)) {
      if (Double.class.isAssignableFrom(cls)) {
        return createDoubleRangeQuery(name, value, type, minInclusive, maxInclusive);
      } else if (Float.class.isAssignableFrom(cls)) {
        return createFloatRangeQuery(name, value, type, minInclusive, maxInclusive);
      } else if (Long.class.isAssignableFrom(cls)) {
        return createLongRangeQuery(name, value, type, minInclusive, maxInclusive);
      } else {
        return createIntRangeQuery(name, value, type, minInclusive, maxInclusive);
      }
    } else if (String.class.isAssignableFrom(cls)) {
      return createStringRangeQuery(name, value, type, minInclusive, maxInclusive);
    } else {
      throw new IllegalArgumentException(format("%s: Can not do range query on field %s of type %s" +
        "only long, int, double, float types are supported", visitorName, name, cls.getName()));
    }
  }

  private SearchQuery createIntRangeQuery(final String name, final Object value,
                                    final ComparisonOperator type, final boolean minInclusive, final boolean maxInclusive) {
    final Integer intValue = (Integer) value;

    final Integer minValue = (type != RSQLOperators.LESS_THAN && type != RSQLOperators.LESS_THAN_OR_EQUAL)
        ? intValue : null;
    final Integer maxValue = (type != RSQLOperators.GREATER_THAN && type != RSQLOperators.GREATER_THAN_OR_EQUAL)
        ? intValue : null;

    return SearchQueryUtils.newRangeInt(name, minValue, maxValue, minInclusive, maxInclusive);
  }

  private SearchQuery createLongRangeQuery(final String name, final Object value,
                                     final ComparisonOperator type, final boolean minInclusive, final boolean maxInclusive) {
    final Long longValue = (Long) value;

    final Long minValue = (type != RSQLOperators.LESS_THAN && type != RSQLOperators.LESS_THAN_OR_EQUAL)
        ? longValue : null;
    final Long maxValue = (type != RSQLOperators.GREATER_THAN && type != RSQLOperators.GREATER_THAN_OR_EQUAL)
        ? longValue : null;

    return SearchQueryUtils.newRangeLong(name, minValue, maxValue, minInclusive, maxInclusive);
  }

  private SearchQuery createDoubleRangeQuery(final String name, final Object value,
                                       final ComparisonOperator type, final boolean minInclusive, final boolean maxInclusive) {
    final Double doubleValue = (Double) value;

    final Double minValue = (type != RSQLOperators.LESS_THAN && type != RSQLOperators.LESS_THAN_OR_EQUAL)
        ? doubleValue : null;
    final Double maxValue = (type != RSQLOperators.GREATER_THAN && type != RSQLOperators.GREATER_THAN_OR_EQUAL)
        ? doubleValue : null;

    return SearchQueryUtils.newRangeDouble(name, minValue, maxValue, minInclusive, maxInclusive);
  }

  private SearchQuery createFloatRangeQuery(final String name, final Object value,
                                      final ComparisonOperator type, final boolean minInclusive, final boolean maxInclusive) {
    final Float floatValue = (Float) value;

    final Float minValue = (type != RSQLOperators.LESS_THAN && type != RSQLOperators.LESS_THAN_OR_EQUAL)
        ? floatValue : null;
    final Float maxValue = (type != RSQLOperators.GREATER_THAN && type != RSQLOperators.GREATER_THAN_OR_EQUAL)
        ? floatValue : null;

    return SearchQueryUtils.newRangeFloat(name, minValue, maxValue, minInclusive, maxInclusive);
  }

  private SearchQuery createStringRangeQuery(final String name, final Object value,
                                       final ComparisonOperator type, final boolean minInclusive, final boolean maxInclusive) {
    final String stringValue = (String)value;

    final String minValue = (type != RSQLOperators.LESS_THAN && type != RSQLOperators.LESS_THAN_OR_EQUAL)
        ? stringValue : null;
    final String maxValue = (type != RSQLOperators.GREATER_THAN && type != RSQLOperators.GREATER_THAN_OR_EQUAL)
        ? stringValue : null;

    return SearchQueryUtils.newRangeTerm(name, minValue, maxValue, minInclusive, maxInclusive);
  }

  private SearchQuery createBooleanFieldQuery(final String name, final Object value) {
    final Boolean booleanValue = (Boolean)value;
    return SearchQueryUtils.newTermQuery(name, booleanValue.toString().toUpperCase());
  }

  private SearchQuery createMatchAllFieldsQuery(final String query, FilterIndexMapping keys) {
    final ImmutableList.Builder<SearchQuery> builder = ImmutableList.builder();
    for (final String name : keys.getSearchAllIndexKeys()) {
      final String value;
      if (query.contains("*")) {
        value = query;
      } else {
        value = format("*%s*", query);
      }
      builder.add(SearchQueryUtils.newWildcardQuery(name, value));
    }

    return SearchQueryUtils.or(builder.build());
  }

  private void checkAtleastOneArgument(final ComparisonNode node) {
    if (node.getArguments().size() < 1) {
      throw new IllegalArgumentException(format("%s: Invalid search operator %s, needs at least one argument",
        visitorName, node.toString()));
    }
  }

  private List<Object> validate(final ComparisonNode node, final String name, final Class<?> clazz) throws IllegalArgumentException {
    checkAtleastOneArgument(node);
    final List<Object> arguments = new ArrayList<>();
    // match all fields doesn't need name or field type
    if (node.getOperator().equals(CONTAINS)) {
      arguments.addAll(node.getArguments());
      return arguments;
    }
    // other operators name and clazz must be non-null
    if (name == null) {
      throw new IllegalArgumentException(format("%s: Invalid search operator %s, unknown field %s",
        visitorName, node.toString(), node.getSelector()));
    }
    if (clazz == null) {
      throw new IllegalArgumentException(format("%s: Invalid search operator %s, unknown type for field %s",
        visitorName, node.toString(), node.getSelector()));
    }
    // Only numeric, boolean and string values are supported
    if (Number.class.isAssignableFrom(clazz)) {
      if (Long.class.isAssignableFrom(clazz)) {
        for (final String arg: node.getArguments()) {
          arguments.add(Long.parseLong(arg));
        }
        return arguments;
      }
      if (Integer.class.isAssignableFrom(clazz)) {
        for (final String arg: node.getArguments()) {
          arguments.add(Integer.parseInt(arg));
        }
        return arguments;
      }
      if (Float.class.isAssignableFrom(clazz)) {
        for (final String arg: node.getArguments()) {
          arguments.add(Float.parseFloat(arg));
        }
        return arguments;
      }
      if (Double.class.isAssignableFrom(clazz)) {
        for (final String arg: node.getArguments()) {
          arguments.add(Double.parseDouble(arg));
        }
        return arguments;
      }
      throw new IllegalArgumentException(format("%s: Invalid numeric field type %s for field %s in search operator %s, " +
        "only long, int, double, float types are supported", visitorName, clazz.toString(), name, node.toString()));
    } else if (Boolean.class.isAssignableFrom(clazz)) {
      for (final String arg: node.getArguments()) {
        arguments.add(Boolean.parseBoolean(arg));
      }
      return arguments;
    } else if (String.class.isAssignableFrom(clazz)) {
      arguments.addAll(node.getArguments());
    } else {
      throw new IllegalArgumentException(format("%s: Invalid field type %s for field %s in search operator %s, " +
          "only numeric, string or boolean types are supported", visitorName, clazz.toString(), name, node.toString()));
    }
    return arguments;
  }
}
