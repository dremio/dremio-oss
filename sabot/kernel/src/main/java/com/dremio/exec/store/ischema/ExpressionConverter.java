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
package com.dremio.exec.store.ischema;

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlFunction;

/**
 * Enables conversion of a filter condition into a search query and remainder for pushdown purposes.
 */
public final class ExpressionConverter {
  private static final ImmutableMap<String, IndexKey> FIELDS =
      ImmutableMap.of(
          "TABLE_SCHEMA".toLowerCase(), DatasetIndexKeys.UNQUOTED_SCHEMA,
          "TABLE_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_NAME,
          "SCHEMA_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_SCHEMA
          // Don't support columns because the filtering pattern with lucene is too complex.
          );
  private static final ImmutableMap<String, IndexKey> LC_FIELDS =
      ImmutableMap.of(
          "TABLE_SCHEMA".toLowerCase(), DatasetIndexKeys.UNQUOTED_LC_SCHEMA,
          "TABLE_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_LC_NAME,
          "SCHEMA_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_LC_SCHEMA);

  private static final ImmutableSet<String> INDEX_COMPATIBLE_FUNCTIONS =
      ImmutableSet.of("LOWER", "UPPER", "LCASE", "UCASE");

  private ExpressionConverter() {}

  public static PushdownResult pushdown(
      RexBuilder rexBuilder, RelDataType rowType, RexNode condition) {
    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
    List<RexNode> remainders = new ArrayList<>();
    List<NodePushdown> found = new ArrayList<>();

    Visitor visitor = new Visitor(rowType);
    for (RexNode n : conjuncts) {
      NodePushdown q = n.accept(visitor);
      if (q == null) {
        remainders.add(n);
      } else {
        if (!q.isExactFilter()) {
          // If we pushed down a filter to the scan, but that scan returns a superset of the real
          // results, we need to maintain the filter in the query plan as well.
          // E.g. for WHERE UPPER(TABLE_NAME) = 'abc', we push down LC_SEARCH_NAME=abc, but we still
          // need to execute the filter so no rows get returned.
          remainders.add(n);
        }
        found.add(q);
      }
    }

    if (found.isEmpty()) {
      return new PushdownResult(null, condition);
    }

    RexNode remainder = RexUtil.composeConjunction(rexBuilder, remainders, true);

    if (found.size() == 1) {
      return new PushdownResult(found.get(0).getQuery(), remainder);
    } else {
      return new PushdownResult(
          SearchQuery.newBuilder()
              .setAnd(
                  SearchQuery.And.newBuilder()
                      .addAllClauses(
                          found.stream().map(NodePushdown::getQuery).collect(Collectors.toList())))
              .build(),
          remainder);
    }
  }

  private static class Visitor implements RexVisitor<NodePushdown> {

    public Visitor(RelDataType rowType) {
      this.rowType = rowType;
    }

    private final RelDataType rowType;

    @Override
    public NodePushdown visitInputRef(RexInputRef inputRef) {
      return null;
    }

    @Override
    public NodePushdown visitLocalRef(RexLocalRef localRef) {
      return null;
    }

    @Override
    public NodePushdown visitLiteral(RexLiteral literal) {
      return null;
    }

    @Override
    public NodePushdown visitCall(RexCall call) {
      final List<NodePushdown> subs = subs(call.getOperands());

      final LitInput bifunc = litInput(call);
      switch (call.getKind()) {
        case EQUALS:
          {
            if (bifunc == null) {
              return null;
            }
            SearchQuery query =
                SearchQuery.newBuilder()
                    .setEquals(
                        SearchQuery.Equals.newBuilder()
                            .setField(bifunc.field.getIndexFieldName())
                            .setStringValue(bifunc.literal))
                    .build();
            return new NodePushdown(query, bifunc.isFunctionCall);
          }
        case AND:
          {
            if (subs == null) {
              return null;
            }

            SearchQuery query =
                SearchQuery.newBuilder()
                    .setAnd(
                        SearchQuery.And.newBuilder()
                            .addAllClauses(
                                subs.stream()
                                    .map(NodePushdown::getQuery)
                                    .collect(Collectors.toList())))
                    .build();
            return new NodePushdown(query, subs.stream().allMatch(NodePushdown::isExactFilter));
          }
        case OR:
          {
            if (subs == null) {
              return null;
            }

            SearchQuery query =
                SearchQuery.newBuilder()
                    .setOr(
                        SearchQuery.Or.newBuilder()
                            .addAllClauses(
                                subs.stream()
                                    .map(NodePushdown::getQuery)
                                    .collect(Collectors.toList())))
                    .build();
            return new NodePushdown(query, subs.stream().allMatch(NodePushdown::isExactFilter));

            // Lucene doesn't handle NOT expressions well in some cases.
            //      case NOT:
            //        if(subs == null || subs.size() != 1) {
            //          return null;
            //        }
            //        SearchQuery q = subs.get(0);
            //        return SearchQueryUtils.not(subs.get(0));
            //
            //      case NOT_EQUALS:
            //        if(bifunc == null) {
            //          return null;
            //        }
            //        return SearchQueryUtils.not(SearchQueryUtils.newTermQuery(bifunc.field,
            // bifunc.literal));
          }
        case LIKE:
          return handleLike(call);

        default:
          return null;
      }
    }

    private NodePushdown handleLike(RexCall call) {

      List<RexNode> operands = call.getOperands();

      IndexKey indexKey = null;
      String pattern = null;
      String escape = null;
      boolean caseSensitiveIndex = true;

      switch (operands.size()) {
        case 3:
          RexNode op3 = operands.get(2);
          if (op3 instanceof RexLiteral) {
            escape = ((RexLiteral) op3).getValue3().toString();
          } else {
            return null;
          }
          // fall through

        case 2:
          RexNode op1 = operands.get(0);
          if (op1 instanceof RexInputRef) {
            RexInputRef input = ((RexInputRef) op1);
            indexKey =
                FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase());
          } else if (op1 instanceof RexCall && isIndexCompatibleCall((RexCall) op1)) {
            RexInputRef input = (RexInputRef) ((RexCall) op1).getOperands().get(0);
            indexKey =
                LC_FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase());
            caseSensitiveIndex = false;
          }
          if (indexKey == null) {
            return null;
          }

          RexNode op2 = operands.get(1);
          if (op2 instanceof RexLiteral) {
            pattern =
                caseSensitiveIndex
                    ? ((RexLiteral) op2).getValue3().toString()
                    : ((RexLiteral) op2).getValue3().toString().toLowerCase();
          } else {
            return null;
          }
          break;

        default:
          return null;
      }

      SearchQuery query =
          SearchQuery.newBuilder()
              .setLike(
                  SearchQuery.Like.newBuilder()
                      .setField(indexKey.getIndexFieldName())
                      .setPattern(pattern)
                      .setEscape(escape == null ? "" : escape)
                      .setCaseInsensitive(false))
              .build();
      return new NodePushdown(query, caseSensitiveIndex);
    }

    @Override
    public NodePushdown visitOver(RexOver over) {
      return null;
    }

    @Override
    public NodePushdown visitCorrelVariable(RexCorrelVariable correlVariable) {
      return null;
    }

    @Override
    public NodePushdown visitDynamicParam(RexDynamicParam dynamicParam) {
      return null;
    }

    @Override
    public NodePushdown visitRangeRef(RexRangeRef rangeRef) {
      return null;
    }

    @Override
    public NodePushdown visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public NodePushdown visitSubQuery(RexSubQuery subQuery) {
      return null;
    }

    @Override
    public NodePushdown visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return null;
    }

    @Override
    public NodePushdown visitTableInputRef(RexTableInputRef fieldRef) {
      return null;
    }

    /**
     * Get an input that is a combination of two values, a literal and an index key.
     *
     * @param call
     * @return Null if call does not match expected pattern. Otherwise the LitInput value.
     */
    private LitInput litInput(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 2) {
        return null;
      }

      RexNode first = operands.get(0);
      RexNode second = operands.get(1);

      RexLiteral literal = null;
      RexInputRef input = null;
      boolean caseSensitiveIndex = true;

      if (first instanceof RexLiteral) {
        literal = (RexLiteral) first;
        if (second instanceof RexInputRef) {
          input = (RexInputRef) second;
        } else {
          return null;
        }
      }

      if (second instanceof RexLiteral) {
        literal = (RexLiteral) second;
        if (first instanceof RexInputRef) {
          input = (RexInputRef) first;
        } else if (first instanceof RexCall && isIndexCompatibleCall((RexCall) first)) {
          input = (RexInputRef) ((RexCall) first).getOperands().get(0);
          caseSensitiveIndex = false;
        } else {
          return null;
        }
      }

      if (input == null) {
        return null;
      }

      IndexKey key =
          caseSensitiveIndex
              ? FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase())
              : LC_FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase());

      if (key == null) {
        return null;
      }

      String literalStr =
          caseSensitiveIndex
              ? literal.getValue3().toString()
              : literal.getValue3().toString().toLowerCase();
      return new LitInput(literalStr, key, caseSensitiveIndex);
    }

    private List<NodePushdown> subs(List<RexNode> ops) {
      List<NodePushdown> subQueries = new ArrayList<>();
      for (RexNode n : ops) {
        NodePushdown nodePushdown = n.accept(this);
        if (nodePushdown == null) {
          return null;
        }
        subQueries.add(nodePushdown);
      }

      return subQueries;
    }

    private static boolean isIndexCompatibleCall(RexCall rexCall) {
      if (!(rexCall.op instanceof SqlFunction)) {
        return false;
      }
      String funcName = rexCall.op.getName();
      if (!INDEX_COMPATIBLE_FUNCTIONS.contains(funcName.toUpperCase())) {
        return false;
      }
      return rexCall.getOperands().get(0) instanceof RexInputRef;
    }

    /** An input that is a combination of two values, a literal and an index key. */
    private static final class LitInput {
      private String literal;
      private IndexKey field;
      private boolean isFunctionCall;

      public LitInput(String literal, IndexKey field, boolean isFunctionCall) {
        super();
        this.literal = literal;
        this.field = field;
        this.isFunctionCall = isFunctionCall;
      }
    }
  }

  public static final class PushdownResult {
    private final SearchQuery query;
    private final RexNode remainder;

    public PushdownResult(SearchQuery query, RexNode remainder) {
      super();
      this.query = query;
      this.remainder = remainder;
    }

    public SearchQuery getQuery() {
      return query;
    }

    public RexNode getRemainder() {
      return remainder;
    }
  }

  private static final class NodePushdown {
    private final SearchQuery query;
    private final boolean isExactFilter;

    public NodePushdown(SearchQuery query, boolean isExactFilter) {
      this.query = query;
      this.isExactFilter = isExactFilter;
    }

    private SearchQuery getQuery() {
      return query;
    }

    private boolean isExactFilter() {
      return isExactFilter;
    }
  }
}
