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

import java.util.ArrayList;
import java.util.List;

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

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.google.common.collect.ImmutableMap;

/**
 * Enables conversion of a filter condition into a search query and remainder for pushdown purposes.
 */
public class ExpressionConverter {
  private final static ImmutableMap<String, IndexKey> FIELDS = ImmutableMap.of(
      "TABLE_SCHEMA".toLowerCase(), DatasetIndexKeys.UNQUOTED_SCHEMA,
      "TABLE_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_NAME,
      "SCHEMA_NAME".toLowerCase(), DatasetIndexKeys.UNQUOTED_SCHEMA
      // Don't support columns because the filtering pattern with lucene is too complex.
      );

  private ExpressionConverter() {
  }

  public static PushdownResult pushdown(RexBuilder rexBuilder, RelDataType rowType, RexNode condition) {
    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
    List<RexNode> unused = new ArrayList<>();
    List<SearchQuery> found = new ArrayList<>();

    Visitor visitor = new Visitor(rowType);
    for(RexNode n : conjuncts) {
      SearchQuery q = n.accept(visitor);
      if(q == null) {
        unused.add(n);
      } else {
        found.add(q);
      }
    }

    if(found.isEmpty()) {
      return new PushdownResult(null, condition);
    }

    RexNode remainder = RexUtil.composeConjunction(rexBuilder, unused, true);

    if(found.size() == 1) {
      return new PushdownResult(found.get(0), remainder);
    } else {
      return new PushdownResult(SearchQuery.newBuilder()
        .setAnd(SearchQuery.And.newBuilder()
          .addAllClauses(found))
        .build(),
        remainder);
    }

  }

  private static class Visitor implements RexVisitor<SearchQuery>{

    public Visitor(RelDataType rowType) {
      this.rowType = rowType;
    }

    private final RelDataType rowType;

    @Override
    public SearchQuery visitInputRef(RexInputRef inputRef) {
      return null;
    }

    @Override
    public SearchQuery visitLocalRef(RexLocalRef localRef) {
      return null;
    }

    @Override
    public SearchQuery visitLiteral(RexLiteral literal) {
      return null;
    }


    @Override
    public SearchQuery visitCall(RexCall call) {
      final List<SearchQuery> subs = subs(call.getOperands());

      final LitInput bifunc = litInput(call);
      switch(call.getKind()) {
      case EQUALS:
        if(bifunc == null) {
          return null;
        }
        return SearchQuery.newBuilder()
          .setEquals(SearchQuery.Equals.newBuilder()
            .setField(bifunc.field.getIndexFieldName())
            .setStringValue(bifunc.literal))
          .build();

      case AND:
        if(subs == null) {
          return null;
        }

        return SearchQuery.newBuilder()
          .setAnd(SearchQuery.And.newBuilder()
            .addAllClauses(subs))
          .build();

      case OR:
        if(subs == null) {
          return null;
        }

        return SearchQuery.newBuilder()
          .setOr(SearchQuery.Or.newBuilder()
            .addAllClauses(subs))
          .build();

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
//        return SearchQueryUtils.not(SearchQueryUtils.newTermQuery(bifunc.field, bifunc.literal));

      case LIKE:
        return handleLike(call);

      default:
        return null;
      }

    }

    private SearchQuery handleLike(RexCall call) {

      List<RexNode> operands = call.getOperands();

      IndexKey indexKey = null;
      String pattern = null;
      String escape = null;

      switch(operands.size()) {

      case 3:

        RexNode op3 = operands.get(2);
        if(op3 instanceof RexLiteral) {
          escape = ((RexLiteral) op3).getValue3().toString();
        } else {
          return null;
        }

      case 2:

        RexNode op1 = operands.get(0);
        if(op1 instanceof RexInputRef) {
          RexInputRef input = ((RexInputRef) op1);
          indexKey = FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase());
        }
        if(indexKey == null) {
          return null;
        }

        RexNode op2 = operands.get(1);
        if(op2 instanceof RexLiteral) {
          pattern = ((RexLiteral) op2).getValue3().toString();
        } else {
          return null;
        }

        break;

      default:
        return null;
      }

      return SearchQuery.newBuilder()
        .setLike(SearchQuery.Like.newBuilder()
          .setField(indexKey.getIndexFieldName())
          .setPattern(pattern)
          .setEscape(escape == null ? "" : escape)
          .setCaseInsensitive(false))
        .build();
    }

    @Override
    public SearchQuery visitOver(RexOver over) {
      return null;
    }

    @Override
    public SearchQuery visitCorrelVariable(RexCorrelVariable correlVariable) {
      return null;
    }

    @Override
    public SearchQuery visitDynamicParam(RexDynamicParam dynamicParam) {
      return null;
    }

    @Override
    public SearchQuery visitRangeRef(RexRangeRef rangeRef) {
      return null;
    }

    @Override
    public SearchQuery visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public SearchQuery visitSubQuery(RexSubQuery subQuery) {
      return null;
    }

    @Override
    public SearchQuery visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return null;
    }

    @Override
    public SearchQuery visitTableInputRef(RexTableInputRef fieldRef) {
      return null;
    }

    /**
     * Get an input that is a combination of two values, a literal and an index key.
     * @param call
     * @return Null if call does not match expected pattern. Otherwise the LitInput value.
     */
    private LitInput litInput(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if(operands.size() != 2) {
        return null;
      }

      RexNode first = operands.get(0);
      RexNode second = operands.get(1);

      RexLiteral literal = null;
      RexInputRef input = null;
      boolean literalFirst = true;

      if(first instanceof RexLiteral) {
        literal = (RexLiteral) first;
        if(second instanceof RexInputRef) {
          input = (RexInputRef) second;
          literalFirst = true;
        } else {
          return null;
        }
      }

      if(second instanceof RexLiteral) {
        literal = (RexLiteral) second;
        if(first instanceof RexInputRef) {
          input = (RexInputRef) first;
          literalFirst = false;
        } else {
          return null;
        }
      }

      if(input == null) {
        return null;
      }

      IndexKey key = FIELDS.get(rowType.getFieldList().get(input.getIndex()).getName().toLowerCase());

      if(key == null) {
        return null;
      }

      return new LitInput(literal.getValue3().toString(), key, literalFirst);

    }

    private List<SearchQuery> subs(List<RexNode> ops){
      List<SearchQuery> subQueries = new ArrayList<>();
      for(RexNode n : ops) {
        SearchQuery query = n.accept(this);
        if(query == null) {
          return null;
        }
        subQueries.add(query);
      }

      return subQueries;
    }

    /**
     * An input that is a combination of two values, a literal and an index key.
     *
     * Also identifies if the literal was the first argument. Useful for GT,LT, etc.
     */
    private static class LitInput {
      private String literal;
      private IndexKey field;
      @SuppressWarnings("unused") private boolean literalFirst;

      public LitInput(String literal, IndexKey field, boolean literalFirst) {
        super();
        this.literal = literal;
        this.field = field;
        this.literalFirst = literalFirst;
      }

    }

  }

  public static class PushdownResult {
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
}
