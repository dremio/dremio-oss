/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rules;

import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

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
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSpecialType;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.lucene.queryparser.classic.QueryConverter;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.ElasticsearchStoragePlugin2;
import com.dremio.plugins.elastic.ElasticsearchStoragePluginConfig;
import com.dremio.plugins.elastic.planning.rels.ElasticIntermediateScanPrel;
import com.dremio.service.namespace.StoragePluginId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Query predicate analyzer.
 */
public class PredicateAnalyzer {
  @SuppressWarnings("serial")
  private static final class PredicateAnalyzerException extends RuntimeException {

    public PredicateAnalyzerException(String message) {
      super(message);
    }

    public PredicateAnalyzerException(Throwable cause) {
      super(cause);
    }
  }
  private static final Logger logger = LoggerFactory.getLogger(PredicateAnalyzer.class);

  /**
   * Walks the expression tree, attempting to convert the entire tree into
   * an equivalent Elasticsearch query filter. If an error occurs, or if it
   * is determined that the expression cannot be converted, an exception is
   * thrown and an error message logged.
   * <p/>
   * Callers should catch ExpressionNotAnalyzableException and fall back to not using push-down filters.
   */
  public static QueryBuilder analyze(ElasticIntermediateScanPrel scan, RexNode originalExpression) throws ExpressionNotAnalyzableException {
    try { // guard SchemaField conversion.

      final RexNode expression = SchemaField.convert(originalExpression, scan, ImmutableSet.of(ElasticSpecialType.GEO_POINT, ElasticSpecialType.GEO_SHAPE));
      try {
        QueryExpression e = (QueryExpression) expression.accept(new Visitor(scan.getCluster().getRexBuilder()));

        if (e != null && e.isPartial()) {
          e = CompoundQueryExpression.completeAnd(e, genScriptFilter(expression, scan.getPluginId(), null));
        }
        logger.debug("Predicate: [{}] converted to: [\n{}]", expression, queryAsJson(e.builder()));
        return e != null ? e.builder() : null;
      } catch (Throwable e) {
        // For now, run the old expression conversion to convert a filter into a native elastic construct
        // like a range filter if possible. When this fails, instead construct a filter with a script translation
        // of the filter.
        logger.debug("Fall back to script: [{}]", expression, e);
        return genScriptFilter(expression, scan.getPluginId(), e);
      }

    } catch(Throwable e){
      throw new ExpressionNotAnalyzableException("Unable to analyze expression.", e);
    }
  }

  private static QueryBuilder genScriptFilter(RexNode expression, StoragePluginId pluginId, Throwable cause)
      throws ExpressionNotAnalyzableException {
    try {
      final boolean supportsV5Features = pluginId.getCapabilities().getCapability(ElasticsearchStoragePlugin2.ENABLE_V5_FEATURES);
      final ElasticsearchStoragePluginConfig config = pluginId.getConfig();
      final Script script = ProjectAnalyzer.getScript(
          expression,
          config.isEnablePainless(),
          supportsV5Features,
          config.isEnableScripts());
      return scriptQuery(script);
    } catch (Throwable t) {
      throw new ExpressionNotAnalyzableException(format(
          "Failed to fully convert predicate: [%s] into an elasticsearch filter", expression), cause);
    }
  }

  public static Script getScript(String script, StoragePluginId pluginId) {
    if (pluginId.getCapabilities().getCapability(ElasticsearchStoragePlugin2.ENABLE_V5_FEATURES)) {
      // when returning a painless script, let's make sure we cast to a valid output type.
      return new Script(String.format("(def) (%s)", script), ScriptType.INLINE, "painless", null);
    } else {
      // keeping this so plan matching tests will pass
      return new Script(script, ScriptType.INLINE, "groovy", null);
    }
  }

  private static class Visitor extends RexVisitorImpl<Expression> {

    private final RexBuilder rexBuilder;

    protected Visitor(RexBuilder rexBuilder) {
      super(true);
      this.rexBuilder = rexBuilder;
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
      return new NamedFieldExpression((SchemaField) inputRef);
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
      return super.visitDynamicParam(dynamicParam);
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef) {
      return super.visitRangeRef(rangeRef);
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
      return super.visitFieldAccess(fieldAccess);
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef) {
      return super.visitLocalRef(localRef);
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
      return new LiteralExpression(literal);
    }

    @Override
    public Expression visitOver(RexOver over) {
      return super.visitOver(over);
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
      return super.visitCorrelVariable(correlVariable);
    }

    private boolean supportedRexCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
        case BINARY:
          switch (call.getKind()) {
            case AND:
            case OR:
            case LIKE:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return true;
            default:
              return false;
          }
        case SPECIAL:
          switch (call.getKind()) {
            case CAST:
            case LIKE:
            case OTHER_FUNCTION:
              return true;
            case CASE:
            case SIMILAR:
            default:
              return false;
          }
        case FUNCTION:
          return true;
        case POSTFIX:
          switch (call.getKind()) {
            case IS_NOT_NULL:
            case IS_NULL:
              return true;
            default: // fall through
          }
        case FUNCTION_ID:
        case FUNCTION_STAR:
        case PREFIX: // NOT()
        default:
          return false;
      }
    }

    @Override
    public Expression visitCall(RexCall call) {

      SqlSyntax syntax = call.getOperator().getSyntax();
      if (!supportedRexCall(call)) {
        throw new PredicateAnalyzerException(format("Unsupported call: [%s]", call));
      }

      switch (syntax) {
        case BINARY:
          return binary(call);
        case POSTFIX:
          return postfix(call);
        case SPECIAL:
          switch (call.getKind()) {
            case CAST:
              return cast(call);
            case LIKE:
              SqlLikeOperator likeOperator = (SqlLikeOperator) call.getOperator();
              if (!likeOperator.isNegated()) {
                return binary(call);
              }
              throw new PredicateAnalyzerException(format("Unsupported call: [%s]", call));
            default:
              throw new PredicateAnalyzerException(format("Unsupported call: [%s]", call));
          }
        case FUNCTION:
          if (call.getOperator().getName().equalsIgnoreCase("CONTAINS")) {
            List<Expression> operands = Lists.newArrayList();
            for (RexNode node : call.getOperands()) {
              final Expression nodeExpr = node.accept(this);
              operands.add(nodeExpr);
            }
            String query = convertQueryString(operands.subList(0, operands.size() - 1), operands.get(operands.size() - 1));
            return QueryExpression.create(new NamedFieldExpression(null)).queryString(query);
          }
        default:
          throw new PredicateAnalyzerException(format("Unsupported syntax [%s] for call: [%s]", syntax, call));
      }
    }

    private static String convertQueryString(List<Expression> fields, Expression query) {
      int index = 0;
      Preconditions.checkArgument(query instanceof LiteralExpression, "Query string must be a string literal");
      String queryString = ((LiteralExpression) query).stringValue();
      Map<String, String> fieldMap = Maps.newHashMap();
      for (Expression expr : fields) {
        if (expr instanceof NamedFieldExpression) {
          NamedFieldExpression field = (NamedFieldExpression) expr;
          String fieldIndexString = String.format("$%d", index++);
          fieldMap.put(fieldIndexString, field.getReference());
        }
      }
      try {
        return QueryConverter.convert(queryString, fieldMap);
      } catch (Exception e) {
        throw new PredicateAnalyzerException(e);
      }
    }

    private CastExpression cast(RexCall call) {

      TerminalExpression argument = (TerminalExpression) call.getOperands().get(0).accept(this);
      // Casts do not work for metadata columns
      isMeta(argument, call, true);

      MajorType target;
      switch (call.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          target = Types.optional(MinorType.VARCHAR).toBuilder().setWidth(call.getType().getPrecision()).build();
          break;
        case INTEGER:
          target = Types.optional(MinorType.INT);
          break;
        case BIGINT:
          target = Types.optional(MinorType.BIGINT);
          break;
        case FLOAT:
          target = Types.optional(MinorType.FLOAT4);
          break;
        case DOUBLE:
          target = Types.optional(MinorType.FLOAT8);
          break;
        case DECIMAL:
          throw new PredicateAnalyzerException("Cast to DECIMAL type unsupported");
        default:
          target = Types.optional(MinorType.valueOf(call.getType().getSqlTypeName().getName()));
      }

      return new CastExpression(target, argument);
    }

    private QueryExpression postfix(RexCall call) {
      Preconditions.checkArgument(call.getKind() == SqlKind.IS_NULL || call.getKind() == SqlKind.IS_NOT_NULL);
      if (call.getOperands().size() != 1) {
        throw new PredicateAnalyzerException(format("Unsupported operator: [%s]", call));
      }
      Expression a = call.getOperands().get(0).accept(this);
      // Elasticsearch does not want is null/is not null (exists query) for _id and _index, although it supports for all other metadata column
      isColumn(a, call, ElasticsearchConstants.ID, true);
      isColumn(a, call, ElasticsearchConstants.INDEX, true);
      QueryExpression operand = QueryExpression.create((TerminalExpression)a);
      return call.getKind() == SqlKind.IS_NOT_NULL ? operand.exists() : operand.notExists();
    }

    /**
     * Process a call which is a binary operation, transforming into an equivalent
     * query expression. Note that the incoming call may be either a simple binary
     * expression, such as 'foo > 5', or it may be several simple expressions connected
     * by 'AND' or 'OR' operators, such as 'foo > 5 AND bar = 'abc' AND 'rot' < 1'.
     */
    private QueryExpression binary(RexCall call) {

      // if AND/OR, do special handling
      if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
        return andOr(call);
      }

      checkForIncompatibleDateTimeOperands(call);

      Preconditions.checkState(call.getOperands().size() == 2);
      final Expression a = call.getOperands().get(0).accept(this);
      final Expression b = call.getOperands().get(1).accept(this);

      final SwapResult pair = swap(a, b);
      final boolean swapped = pair.isSwapped();

      // For _id and _index columns, only equals/not_equals work!
      if (isColumn(pair.getKey(), call, ElasticsearchConstants.ID, false)
        || isColumn(pair.getKey(), call, ElasticsearchConstants.INDEX, false)
        || isColumn(pair.getKey(), call, ElasticsearchConstants.UID, false)) {
        switch (call.getKind()) {
          case EQUALS:
          case NOT_EQUALS:
            break;
          default:
            throw new PredicateAnalyzerException("Cannot handle " + call.getKind() + " expression for _id field, " + call);
        }
      }

      switch (call.getKind()) {
        case LIKE:
          // LIKE/regexp cannot handle metadata columns
          isMeta(pair.getKey(), call, true);
          String sqlRegex = RegexpUtil.sqlToRegexLike(pair.getValue().stringValue());
          RexLiteral sqlRegexLiteral = rexBuilder.makeLiteral(sqlRegex);
          LiteralExpression sqlRegexExpression = new LiteralExpression(sqlRegexLiteral);
          return QueryExpression.create(pair.getKey()).like(sqlRegexExpression);
        case EQUALS:
          return QueryExpression.create(pair.getKey()).equals(pair.getValue());
        case NOT_EQUALS:
          return QueryExpression.create(pair.getKey()).notEquals(pair.getValue());
        case GREATER_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gt(pair.getValue());
        case GREATER_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gte(pair.getValue());
        case LESS_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lt(pair.getValue());
        case LESS_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lte(pair.getValue());
        default:
          break;
      }
      throw new PredicateAnalyzerException(format("Unable to handle call: [%s]", call));
    }

    private QueryExpression andOr(RexCall call) {
      QueryExpression[] expressions = new QueryExpression[call.getOperands().size()];
      PredicateAnalyzerException firstError = null;
      boolean partial = false;
      for (int i = 0; i < call.getOperands().size(); i++) {
        try {
          expressions[i] = (QueryExpression) call.getOperands().get(i).accept(this);
          partial |= expressions[i].isPartial();
        } catch (PredicateAnalyzerException e) {
          if (firstError == null) {
            firstError = e;
          }
          partial = true;
        }
      }

      switch (call.getKind()) {
        case OR:
          if (partial) {
            if (firstError != null) {
              throw firstError;
            } else {
              throw new PredicateAnalyzerException(format("Unable to handle call: [%s]", call));
            }
          }
          return CompoundQueryExpression.or(expressions);
        case AND:
          return CompoundQueryExpression.and(partial, expressions);
        default:
          throw new PredicateAnalyzerException(format("Unable to handle call: [%s]", call));
      }
    }


    private static class SwapResult {
      final boolean swapped;
      final TerminalExpression terminal;
      final LiteralExpression literal;
      public SwapResult(boolean swapped, TerminalExpression terminal, LiteralExpression literal) {
        super();
        this.swapped = swapped;
        this.terminal = terminal;
        this.literal = literal;
      }

      public TerminalExpression getKey(){
        return terminal;
      }

      public LiteralExpression getValue(){
        return literal;
      }

      public boolean isSwapped(){
        return swapped;
      }
    }

    /**
     * Swap order of operands such that the literal expression is always on the right.
     * <p/>
     * NOTE: Some combinations of operands are implicitly not supported and will
     * cause an exception to be thrown. For example, we currently do not support
     * comparing a literal to another literal as convention "5 = 5". Nor do we support
     * comparing named fields to other named fields as convention "$0 = $1".
     */
    private static SwapResult swap(Expression left, Expression right) {

      TerminalExpression terminal;
      LiteralExpression literal = expressAsLiteral(left);
      boolean swapped = false;
      if (literal != null) {
        swapped = true;
        terminal = (TerminalExpression) right;
      } else {
        literal = expressAsLiteral(right);
        terminal = (TerminalExpression) left;
      }

      if (literal == null || terminal == null) {
        throw new PredicateAnalyzerException(format(
                "Unexpected combination of expressions [left: %s] [right: %s]", left, right));
      }

      if (CastExpression.isCastExpression(terminal)) {
        terminal = CastExpression.unpack(terminal);
      }

      return new SwapResult(swapped, terminal, literal);
    }

    /**
     * Try to convert a generic expression into a literal expression.
     */
    private static LiteralExpression expressAsLiteral(Expression exp) {

      if (exp instanceof LiteralExpression) {
        return (LiteralExpression) exp;
      }

      if (exp instanceof CastExpression) {
        if (((CastExpression) exp).isCastFromLiteral()) {
          return (LiteralExpression) ((CastExpression) exp).argument;
        }
      }

      return null;
    }

    private static boolean isMeta(Expression exp, RexNode node, boolean throwException) {
      if (!(exp instanceof NamedFieldExpression)) {
        return false;
      }

      final NamedFieldExpression termExp = (NamedFieldExpression) exp;
      if(termExp.isMetaField()){
        if (throwException) {
          throw new PredicateAnalyzerException("Cannot handle metadata field in " + node);
        }
        return true;
      }
      return false;
    }

    private static boolean isColumn(Expression exp, RexNode node, String columnName, boolean throwException) {
      if (!(exp instanceof NamedFieldExpression)) {
        return false;
      }

      final NamedFieldExpression termExp = (NamedFieldExpression) exp;
      if (columnName.equals(termExp.getRootName())) {
        if (throwException) {
          throw new PredicateAnalyzerException("Cannot handle _id field in " + node);
        }
        return true;
      }
      return false;
    }
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  public interface Expression {
  }

  public abstract static class QueryExpression implements Expression {

    public abstract QueryBuilder builder();

    public boolean isPartial() {
      return false;
    }

    public abstract QueryExpression exists();

    public abstract QueryExpression notExists();

    public abstract QueryExpression like(LiteralExpression literal);

    public abstract QueryExpression equals(LiteralExpression literal);

    public abstract QueryExpression notEquals(LiteralExpression literal);

    public abstract QueryExpression gt(LiteralExpression literal);

    public abstract QueryExpression gte(LiteralExpression literal);

    public abstract QueryExpression lt(LiteralExpression literal);

    public abstract QueryExpression lte(LiteralExpression literal);

    public abstract QueryExpression queryString(String query);

    public static QueryExpression create(TerminalExpression expression) {

      if (expression instanceof NamedFieldExpression) {
        return new SimpleQueryExpression((NamedFieldExpression) expression);
      } else {
        throw new PredicateAnalyzerException(format("Unsupported expression: [%s]", expression));
      }
    }
  }

  public static class CompoundQueryExpression extends QueryExpression {

    private final boolean partial;
    private BoolQueryBuilder builder = boolQuery();

    public static CompoundQueryExpression or(QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      for (QueryExpression expression : expressions) {
        bqe.builder.should(expression.builder());
      }
      return bqe;
    }

    /**
     * if partial expression, we will need to complete it with a full filter
     * @param partial whether we partially converted a and for push down purposes.
     * @param expressions
     * @return
     */
    public static CompoundQueryExpression and(boolean partial, QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(partial);
      for (QueryExpression expression : expressions) {
        if (expression != null) { // partial expressions have nulls for missing nodes
          bqe.builder.must(expression.builder());
        }
      }
      return bqe;
    }

    /**
     *
     * @param expression the incomplete expression (but faster using indices)
     * @param builder the full expression (for correctness)
     * @return
     */
    public static CompoundQueryExpression completeAnd(QueryExpression expression, QueryBuilder builder) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      bqe.builder.must(expression.builder());
      bqe.builder.must(builder);
      return bqe;
    }

    private CompoundQueryExpression(boolean partial) {
      this.partial = partial;
    }

    @Override
    public boolean isPartial() {
      return partial;
    }

    @Override
    public QueryBuilder builder() {
      return Preconditions.checkNotNull(builder);
    }

    @Override
    public QueryExpression exists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['exists'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notExists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['notExists'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['like'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['='] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['not'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>='] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<'] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<='] cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression queryString(String query) {
      throw new PredicateAnalyzerException("QueryString cannot be applied to a compound expression");
    }
  }

  public static class SimpleQueryExpression extends QueryExpression {

    private final NamedFieldExpression rel;
    private QueryBuilder builder;

    private String getFieldReference(){
      return rel.getReference();
    }

    public SimpleQueryExpression(NamedFieldExpression rel) {
      this.rel = rel;
    }

    @Override
    public QueryBuilder builder() {
      return Preconditions.checkNotNull(builder);
    }

    @Override
    public QueryExpression exists() {
      builder = existsQuery(getFieldReference());
      return this;
    }

    @Override
    public QueryExpression notExists() {
      builder = boolQuery().mustNot(existsQuery(getFieldReference()));
      return this;
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      builder = regexpQuery(getFieldReference(), literal.stringValue());
      return this;
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = boolQuery()
          .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value)))
          .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value)));
      } else {
        builder = matchQuery(getFieldReference(), value);
      }
      return this;
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = boolQuery()
          .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gt(value)))
          .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value)));
      } else {
        builder = boolQuery().mustNot(matchQuery(getFieldReference(), value));
      }
      return this;
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gt(value));
      return this;
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value));
      return this;
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value));
      return this;
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value));
      return this;
    }

    @Override
    public QueryExpression queryString(String query) {
      builder = queryStringQuery(query);
      return this;
    }
  }


  /**
   * By default, range queries on date/time need use the format of the source to parse the literal. So we need to specify
   * that the literal has "date_time" format
   * @param literal
   * @param rangeQueryBuilder
   * @return
   */
  private static RangeQueryBuilder addFormatIfNecessary(LiteralExpression literal, RangeQueryBuilder rangeQueryBuilder) {
    if (literal.value() instanceof GregorianCalendar) {
      rangeQueryBuilder.format("date_time");
    }
    return rangeQueryBuilder;
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  public interface TerminalExpression extends Expression {
  }

  public static final class NamedFieldExpression implements TerminalExpression {

    private final SchemaField schemaField;

    public NamedFieldExpression(SchemaField schemaField) {
      this.schemaField = schemaField;
    }

    public String getRootName(){
      return schemaField.getPath().getRootSegment().getPath();
    }

    public boolean isMetaField(){
      return ElasticsearchConstants.META_COLUMNS.contains(getRootName());
    }

    public String getReference(){
      return schemaField.getPath().getAsUnescapedPath();
    }
  }

  public static final class CastExpression implements TerminalExpression {

    public final MajorType target;
    public final TerminalExpression argument;

    public CastExpression(MajorType target, TerminalExpression argument) {
      this.target = target;
      this.argument = argument;
    }

    public boolean isCastFromLiteral() {
      return argument instanceof LiteralExpression;
    }

    public static TerminalExpression unpack(TerminalExpression exp) {
      if (!(exp instanceof CastExpression)) {
        return exp;
      }
      return ((CastExpression) exp).argument;
    }

    public static boolean isCastExpression(Expression exp) {
      return (exp instanceof CastExpression);
    }
  }

  public static final class LiteralExpression implements TerminalExpression {

    public final RexLiteral literal;

    public LiteralExpression(RexLiteral literal) {
      this.literal = literal;
    }

    Object value() {

      if (isIntegral()) {
        return longValue();
      } else if (isFloatingPoint()) {
        return doubleValue();
      } else if (isBoolean()) {
        return booleanValue();
      } else if (isString()) {
        return RexLiteral.stringValue(literal);
      } else {
        return rawValue();
      }
    }

    public boolean isIntegral() {
      return SqlTypeName.INT_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isFloatingPoint() {
      return SqlTypeName.APPROX_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isBoolean() {
      return SqlTypeName.BOOLEAN_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isString() {
      return SqlTypeName.CHAR_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public long longValue() {
      return ((Number) literal.getValue()).longValue();
    }

    public double doubleValue() {
      return ((Number) literal.getValue()).doubleValue();
    }

    public boolean booleanValue() {
      return RexLiteral.booleanValue(literal);
    }

    public String stringValue() {
      return RexLiteral.stringValue(literal);
    }

    public Object rawValue() {
      return literal.getValue();
    }
  }

  public static String queryAsJson(QueryBuilder query) throws IOException {
    XContentBuilder x = XContentFactory.jsonBuilder();
    x.prettyPrint().lfAtEnd();
    query.toXContent(x, ToXContent.EMPTY_PARAMS);
    return x.string();
  }

  /**
   * If one operand in a binary operator is a DateTime type, but the other isn't, we should not push down the predicate
   * @param call
   */
  public static void checkForIncompatibleDateTimeOperands(RexCall call) {
    RelDataType op1 = call.getOperands().get(0).getType();
    RelDataType op2 = call.getOperands().get(1).getType();
    if (
        (SqlTypeFamily.DATETIME.contains(op1) && !SqlTypeFamily.DATETIME.contains(op2)) ||
        (SqlTypeFamily.DATETIME.contains(op2) && !SqlTypeFamily.DATETIME.contains(op1)) ||
        (SqlTypeFamily.DATE.contains(op1) && !SqlTypeFamily.DATE.contains(op2)) ||
        (SqlTypeFamily.DATE.contains(op2) && !SqlTypeFamily.DATE.contains(op1)) ||
        (SqlTypeFamily.TIMESTAMP.contains(op1) && !SqlTypeFamily.TIMESTAMP.contains(op2)) ||
        (SqlTypeFamily.TIMESTAMP.contains(op2) && !SqlTypeFamily.TIMESTAMP.contains(op1)) ||
        (SqlTypeFamily.TIME.contains(op1) && !SqlTypeFamily.TIME.contains(op2)) ||
        (SqlTypeFamily.TIME.contains(op2) && !SqlTypeFamily.TIME.contains(op1)))
    {
      throw new PredicateAnalyzerException("Cannot handle " + call.getKind() + " expression for _id field, " + call);
    }
  }



}
