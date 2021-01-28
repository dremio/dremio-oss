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
package com.dremio.common.rel2sql;

import static org.apache.calcite.util.Util.skipLast;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.rel2sql.utilities.OrderByAliasProcessor;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.handlers.OverUtils;
import com.dremio.exec.planner.sql.handlers.ShortenJdbcColumnAliases;
import com.dremio.service.Pointer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Dremio base implementation for converting a Rel tree to a SQL tree
 * that can be converted to an RDBMS-specific dialect.
 */
public class DremioRelToSqlConverter extends RelToSqlConverter {
  private static final class SqlCharStringLiteralWithCollation extends SqlCharStringLiteral {
    private final SqlCollation collation;

    private SqlCharStringLiteralWithCollation(NlsString val, SqlParserPos pos, SqlCollation collation) {
      super(val, pos);
      this.collation = collation;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      super.unparse(writer, leftPrec, rightPrec);

      if (collation != null) {
        collation.unparse(writer, leftPrec, rightPrec);
      }
    }
  }

  /**
   * A version of SqlSumEmptyIsZeroAggFunction that has its named set to SUM() so that
   * it can be pushed to RDBMSes correctly.
   */
  private static final SqlSumEmptyIsZeroAggFunction SUM0_FUNCTION = new SqlSumEmptyIsZeroAggFunction() {
    @Override
    public String getName() {
      return "SUM";
    }
  };

  protected final Deque<DremioContext> outerQueryAliasContextStack = new ArrayDeque<>();

  // A stack to keep track of window operators in the tree.
  // This is used in builder as context information to figure out whether it needs a new subquery.
  protected final Deque<RelNode> windowStack = new ArrayDeque<>();

  private int projectLevel = 0;

  public DremioRelToSqlConverter(DremioSqlDialect dremioDialect) {
    super(dremioDialect);
  }

  /**
   * Gets the dialect as a DremioSqlDialect.
   */
  protected DremioSqlDialect getDialect() {
    return (DremioSqlDialect) this.dialect;
  }

  /**
   * Gets the converter as DremioRelToSqlConverter.
   */
  public DremioRelToSqlConverter getDremioRelToSqlConverter() {
    return this;
  }

  protected boolean isSubQuery() {
    return !outerQueryAliasContextStack.isEmpty();
  }

  // SqlImplementor overrides

  /**
   * Factory method for Result
   */
  @Override
  public Result makeNewResult(SqlNode node, Collection<Clause> clauses, String neededAlias,
                              RelDataType neededType, Map<String, RelDataType> aliases) {
    return new Result(node, clauses, neededAlias, neededType, aliases);
  }

  /**
   * Creates a result based on a single relational expression.
   */
  public Result result(SqlNode node, Collection<Clause> clauses, RelNode rel,
                       RelDataType neededType, Map<String, RelDataType> aliases) {
    assert aliases == null
      || aliases.size() < 2
      || aliases instanceof LinkedHashMap
      || aliases instanceof ImmutableMap
      : "must use a Map implementation that preserves order";
    final String alias2;
    final String alias4;
    final boolean sameRowType =
      neededType != null && neededType.equals(rel.getRowType());

    // No need to create a new alias if only one alias
    if (aliases != null && aliases.size() == 1 && sameRowType) {
      alias2 = null;
      alias4 = aliases.keySet().iterator().next();
    } else {
      alias2 = SqlValidatorUtil.getAlias(node, -1);
      final String alias3 = alias2 != null ? alias2 : "t";

      String tableAlias = SqlValidatorUtil.uniquify(
        alias3, aliasSet, SqlValidatorUtil.EXPR_SUGGESTER);
      if (getDialect().getIdentifierLengthLimit() != null && tableAlias.length() > getDialect().getIdentifierLengthLimit()) {
        tableAlias = SqlValidatorUtil.uniquify(
          alias3, aliasSet, ShortenJdbcColumnAliases.SHORT_ALIAS_SUGGESTER);
      }
      alias4 = tableAlias;
    }

    final Map<String, RelDataType> newAliases;
    final RelDataType newDataType;

    if (aliases != null && !aliases.isEmpty() && sameRowType) {
      Preconditions.checkArgument(neededType != null,
        "Cannot have non-null aliases when RelDataType is null");
      newAliases = aliases;
      newDataType = neededType;
    } else {
      newAliases = ImmutableMap.of(alias4, rel.getRowType());
      newDataType = rel.getRowType();
    }

    if (aliases != null
      && !aliases.isEmpty()
      && (!dialect.hasImplicitTableAlias()
      || aliases.size() > 1)) {
      return makeNewResult(node, clauses, alias4, newDataType, newAliases);
    }

    final String alias5;
    if (alias2 == null
      || !alias2.equals(alias4)
      || !dialect.hasImplicitTableAlias()) {
      alias5 = alias4;
    } else {
      alias5 = null;
    }
    return makeNewResult(node, clauses, alias5, newDataType, newAliases);
  }

  @Override
  SqlSelect wrapSelect(SqlNode node) {
    // We override Calcite's wrapSelect to fail fast here. Dremio needs the Result for its wrapSelect implementation,
    // so it implements it as an instance method on DremioRelToSqlConverter.Result as wrapSelectAndPushOrderBy()
    //
    // We need to replace any calls to SqlImplementor#wrapSelect() with
    // DremioRelToSqlConverter.Result#wrapSelectAndPushOrderBy().
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a result based on a single relational expression.
   */
  @Override
  public Result result(SqlNode node, Collection<Clause> clauses,
                       RelNode rel, Map<String, RelDataType> aliases) {
    return result(node, clauses, rel, rel.getRowType(), aliases);
  }

  /**
   * Process the Result from visiting a Project node's child.
   *
   * @param project     The project node.
   * @param childResult The Result from calling visit() on the child of project.
   * @return The Result of visiting the Project node itself.
   */
  protected DremioRelToSqlConverter.Result processProjectChild(Project project, DremioRelToSqlConverter.Result childResult) {
    // unlike the super impl #visit(Project), this expands star ("*") if additional push downs are enabled

    Pointer<Boolean> isPartial = new Pointer<>(false);
    project.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        if (scan instanceof ScanRelBase) {
          ScanRelBase tableScan = (ScanRelBase) scan;
          if (tableScan.getTableMetadata().getSchema().getFieldCount() != tableScan.getProjectedColumns().size()) {
            isPartial.value = true;
          }
        }
        return scan;
      }
    });

    PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(project.getCluster());
    if (plannerSettings != null &&
      !plannerSettings.getOptions().getOption(PlannerSettings.JDBC_PUSH_DOWN_PLUS) &&
      !isPartial.value &&
      isStar(project.getChildExps(), project.getInput().getRowType(), project.getRowType())) {
      return childResult;
    }

    final DremioRelToSqlConverter.Builder builder =
      childResult.builder(project, SqlImplementor.Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : project.getChildExps()) {
      SqlNode sqlExpr = builder.context.toSql(null, simplifyDatetimePlus(ref, project.getCluster().getRexBuilder()));
      if (1 == projectLevel) {
        if ((getDialect().shouldInjectNumericCastToProject() && isDecimal(ref.getType())) ||
            (getDialect().shouldInjectApproxNumericCastToProject() && isApproximateNumeric(ref.getType()))) {
          if (!((sqlExpr.getKind() == SqlKind.CAST) || (sqlExpr.getKind() == SqlKind.AS && ((SqlBasicCall) sqlExpr).operand(0).getKind() == SqlKind.CAST))) {
            // Add an explicit cast around this projection.
            sqlExpr = SqlStdOperatorTable.CAST.createCall(POS, sqlExpr, getDialect().getCastSpec(ref.getType()));
          }
        }
      }

      addSelect(selectList, sqlExpr, project.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  // Visitor overrides
  @Override
  public DremioRelToSqlConverter.Result visit(Project e) {
    ++projectLevel;
    try {
      DremioRelToSqlConverter.Result x = (DremioRelToSqlConverter.Result) visitChild(0, e.getInput());
      return processProjectChild(e, x);
    } finally {
      --projectLevel;
    }
  }

  /**
   * @see #dispatch
   */
  @Override
  public SqlImplementor.Result visit(Join e) {
    final DremioRelToSqlConverter.Result leftResult = (DremioRelToSqlConverter.Result) visitChild(0, e.getLeft()).resetAlias();
    final DremioRelToSqlConverter.Result rightResult = (DremioRelToSqlConverter.Result) visitChild(1, e.getRight()).resetAlias();

    final SqlLiteral condType = JoinConditionType.ON.symbol(POS);
    final JoinType joinType = joinType(e.getJoinType());
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    final Context joinContext = leftContext.implementor().joinContext(leftContext, rightContext);
    final RexNode condition = e.getCondition() == null ?
      null :
      this.simplifyDatetimePlus(e.getCondition(), e.getCluster().getRexBuilder());
    final SqlNode sqlCondition = joinContext.toSql(null, condition);

    final SqlNode join =
      new SqlJoin(POS,
        leftResult.asFrom(),
        SqlLiteral.createBoolean(false, POS),
        joinType.symbol(POS),
        rightResult.asFrom(),
        condType,
        sqlCondition);
    return result(join, leftResult, rightResult);
  }

  /**
   * @see #dispatch
   */
  public SqlImplementor.Result visit(ScanRelBase scan) {
    List<String> tableQualifiedName = scan.getTable().getQualifiedName();
    int index = tableQualifiedName.size() > 1 ? 1 /* Full path minus plugin name */ : 0;
    SqlIdentifier tableName = new SqlIdentifier(
      ImmutableList.copyOf(scan.getTable().getQualifiedName().listIterator(index)),
      SqlParserPos.ZERO);
    return result(tableName, ImmutableList.of(Clause.FROM), scan, null);
  }

  /**
   * @see #dispatch
   */
  @Override
  public Result visit(Aggregate e) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final Result x = (Result) visitChild(0, e.getInput());

    boolean shouldInjectGroupByToCurrentQuery = false;
    final boolean containsGroupBy = x.clauses.contains(Clause.GROUP_BY);

    if (!containsGroupBy && e.getInput() instanceof Project) {
      final DremioContext context = (DremioContext) x.builder(e).context;

      // Cycle through the group by sets to see if there is a group by on an aggregate call.
      // If so, we do not want to do "group by COUNT(*)", so need to make the current Result
      // a subquery.
      final boolean groupByWithSqlCall = context.containsSqlCall(e.getGroupSet());

      boolean aggCallWithSqlCall = false;

      // Cycle through the aggregate calls to see if there is an aggregate call on an
      // aggregate call, e.g. MIN(SUM(X)). If so, we do not want to pushdown nested
      // aggregate calls to databases that do not support nested aggregate calls.
      // In this case, separate them out into subqueries.
      if (!groupByWithSqlCall && !dialect.supportsNestedAggregations()) {
        for (AggregateCall aggCall : e.getAggCallList()) {
          aggCallWithSqlCall = context.containsSqlCall(aggCall.getArgList());
          if (aggCallWithSqlCall) {
            break;
          }
        }
      }

      shouldInjectGroupByToCurrentQuery = !groupByWithSqlCall && !aggCallWithSqlCall;
    } else if (dialect.supportsNestedAggregations()
      && e.getInput() instanceof Project
      && e.getGroupSet().isEmpty()) {
      // Applying an aggregate without grouping on top of another aggregate function
      // is the same as a double aggregation
      // eg SELECT MAX(SUM(c1)) FROM table GROUP BY c2
      final DremioContext context = (DremioContext) x.builder(e).context;

      boolean hasOnlyDoubleAggregates = true;
      for (AggregateCall aggCall : e.getAggCallList()) {
        hasOnlyDoubleAggregates = hasOnlyDoubleAggregates &&
          context.containsAggregateCall(aggCall.getArgList());

        if (!hasOnlyDoubleAggregates) {
          break;
        }
      }
      shouldInjectGroupByToCurrentQuery = hasOnlyDoubleAggregates;
    }

    final Builder builder;
    if (shouldInjectGroupByToCurrentQuery) {
      builder = x.builder(e);
      builder.clauses.add(Clause.GROUP_BY);
    } else {
      builder = x.builder(e, Clause.GROUP_BY);
    }

    getDremioRelToSqlConverter().generateGroupBy(builder, e);
    return builder.result();
  }

  protected void generateGroupBy(DremioRelToSqlConverter.Builder builder, Aggregate e) {
    List<SqlNode> groupByList = Expressions.list();
    final List<SqlNode> selectList = new ArrayList<>();
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      addGroupBy(groupByList, field);
    }

    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = ((DremioContext) builder.context).toSql(aggCall, e);

      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode = dialect.
          rewriteSingleValueExpr(aggCallSqlNode);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
  }

  /**
   * @see #dispatch
   */
  @Override
  public Result visit(Sort e) {
    Result x = (Result) visitChild(0, e.getInput());
    x = visitOrderByHelper(x, e);
    x = visitFetchAndOffsetHelper(x, e);
    return x;
  }

  protected Result visitFetchAndOffsetHelper(Result x, Sort e) {
    if (e.fetch != null) {
      Builder builder = x.builder(e, Clause.FETCH);
      builder.setFetch(builder.context.toSql(null, e.fetch));
      x = builder.result();
    }
    if (e.offset != null) {
      Builder builder = x.builder(e, Clause.OFFSET);
      builder.setOffset(builder.context.toSql(null, e.offset));
      x = builder.result();
    }
    return x;
  }

  protected Result visitOrderByHelper(Result x, Sort e) {
    boolean orderBySqlCall = false;
    final Builder tempBuilder = x.builder(e);
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      final SqlNode orderByField = tempBuilder.context.field(field.getFieldIndex());
      if (orderByField instanceof SqlCall) {
        orderBySqlCall = true;
        break;
      }
    }

    Builder builder = x.builder(e, orderBySqlCall, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      x = builder.result();
    }
    return x;
  }

  /**
   * @see #dispatch
   */
  @Override
  public SqlImplementor.Result visit(Values e) {
    if (getDialect().getValuesDummyTable() == null) {
      return super.visit(e);
    }


    // TODO(DX-12875): Generate a multi-row VALUES clause to push to the source
    // instead of using UNION statements.
    final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);

    final SqlNode fromNode = new SqlIdentifier(getDialect().getValuesDummyTable(), SqlParserPos.ZERO);
    final List<String> fields = e.getRowType().getFieldNames();
    final List<SqlSelect> selects = new ArrayList<>();
    for (List<RexLiteral> tuple : e.getTuples()) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (Pair<RexLiteral, String> literal : Pair.zip(tuple, fields)) {
        selectList.add(
          SqlStdOperatorTable.AS.createCall(
            POS,
            context.toSql(null, literal.left),
            new SqlIdentifier(literal.right, POS)));
      }
      selects.add(
        new SqlSelect(POS, SqlNodeList.EMPTY,
          new SqlNodeList(selectList, POS), fromNode, null, null,
          null, null, null, null, null));
    }
    SqlNode query = null;
    for (SqlSelect select : selects) {
      if (query == null) {
        query = select;
      } else {
        query = SqlStdOperatorTable.UNION_ALL.createCall(POS, query,
          select);
      }
    }
    return result(query, clauses, e, null, null);
  }

  // Calcite overrides
  @Override
  public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
    final String name = rowType.getFieldNames().get(selectList.size());
    final String alias = SqlValidatorUtil.getAlias(node, -1);
    if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }

  protected void addGroupBy(List<SqlNode> groupByList, SqlNode node) {
    groupByList.add(node);
  }

  // Dremio-specific

  /**
   * Rewrite the given Window clause to add data-source specific customizations.
   */
  public SqlWindow adjustWindowForSource(DremioContext
                                           context, SqlAggFunction op, SqlWindow window) {
    return window;
  }

  /**
   * Callback function to rewrite directly override RexCall nodes when converting them to SqlNodes.
   * This differs from the Calcite-supplied unparseCall() method in SqlDialect in that because
   * you have access to the RexCall node, you have semantic information about parameters such as
   * their types, while with unparseCall() you only have syntactic information.
   * <p>
   * Return null to defer this to the DremioContextMoved
   * classes.
   */
  public SqlNode convertCallToSql(DremioContext
                                    context, RexProgram program, RexCall call, boolean ignoreCast) {
    return null;
  }

  /**
   * Returns true if the order by clause can be pushed to the outer query.
   */
  public boolean shouldPushOrderByOut() {
    return true;
  }

  /**
   * Returns a SqlWindow that adds ORDER BY if op is one of the given functions.
   */
  protected static SqlWindow addDummyOrderBy(SqlWindow window, DremioContext
    context, SqlAggFunction op, List<SqlAggFunction> opsToAddClauseFor) {
    if (!SqlNodeList.isEmptyList(window.getOrderList())) {
      return window;
    }

    // Add the ORDER BY if op is one of the given functions.
    for (SqlAggFunction function : opsToAddClauseFor) {
      if (function == op) {
        SqlNodeList dummyOrderByList = new SqlNodeList(POS);
        dummyOrderByList.add(context.field(0));

        return SqlWindow.create(window.getDeclName(), window.getRefName(), window.getPartitionList(),
          dummyOrderByList, SqlLiteral.createBoolean(window.isRows(), POS), window.getLowerBound(),
          window.getUpperBound(), SqlLiteral.createBoolean(window.isAllowPartial(), POS), POS);
      }
    }

    return window;
  }

  //~ Context overrides
  @Override
  public SqlImplementor.Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
    return new DremioAliasContext(aliases, qualified);
  }

  @Override
  public SqlImplementor.Context joinContext(Context leftContext, Context rightContext) {
    return new DremioJoinContext(leftContext, rightContext);
  }

  @Override
  public Context matchRecognizeContext(Context context) {
    return new DremioMatchRecognizeContext(((DremioAliasContext) context).getAliases());
  }

  public Context selectListContext(SqlNodeList selectList) {
    return new DremioSelectListContext(selectList);
  }

  public SqlNode createIntervalLiteral(final RexLiteral interval) {
    final BigDecimal intervalValue = interval.getValueAs(BigDecimal.class);
    final SqlTypeFamily family = interval.getTypeName().getFamily();
    final SqlIntervalQualifier qualifier = interval.getType().getIntervalQualifier();

    if (intervalValue == null) {
      return null;
    }

    final TimeUnit unit;
    if (qualifier.getEndUnit() != null) {
      unit = qualifier.getEndUnit();
    } else {
      unit = qualifier.getStartUnit();
    }

    final BigDecimal multiplier;
    switch (unit) {
      case YEAR:
      case MONTH:
        multiplier = BigDecimal.ONE;
        assert family == SqlTypeFamily.INTERVAL_YEAR_MONTH;
        break;
      case QUARTER:
        multiplier = new BigDecimal(3);
        assert family == SqlTypeFamily.INTERVAL_YEAR_MONTH;
        break;

      case WEEK:
        multiplier = new BigDecimal(7);
        assert family == SqlTypeFamily.INTERVAL_DAY_TIME;
        break;
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
        multiplier = BigDecimal.ONE;
        assert family == SqlTypeFamily.INTERVAL_DAY_TIME;
        break;
      default:
        throw new IllegalArgumentException("Interval not supported for " + unit);
    }
    final BigDecimal newIntervalValue = intervalValue.multiply(multiplier).divide(unit.multiplier);

    return SqlLiteral.createInterval(
      1, // Sign will be inferred by passing in the string representation of the bignum below.
      newIntervalValue.toString(),
      new SqlIntervalQualifier(unit, null, qualifier.getParserPosition()),
      qualifier.getParserPosition());
  }

  public SqlNode createLeftCall(SqlOperator op, List<SqlNode> nodeList) {
    if (nodeList.size() == 2) {
      return op.createCall(new SqlNodeList(nodeList, POS));
    }
    final List<SqlNode> butLast = skipLast(nodeList);
    final SqlNode last = nodeList.get(nodeList.size() - 1);
    final SqlNode call = createLeftCall(op, butLast);
    return op.createCall(new SqlNodeList(ImmutableList.of(call, last), POS));
  }

  /**
   * Base context class used for DremioRelToSqlConverter.
   */
  public abstract class DremioContext extends SqlImplementor.Context {
    protected DremioContext(int fieldCount) {
      super(fieldCount);
    }

    /**
     * Converts an expression from {@link RexNode} to {@link SqlNode}
     * format.
     *
     * @param program Required only if {@code rex} contains {@link RexLocalRef}
     * @param rex     Expression to convert
     */
    @Override
    public SqlNode toSql(RexProgram program, RexNode rex) {

      switch (rex.getKind()) {
        case LITERAL:
          final RexLiteral literal = (RexLiteral) rex;

          if (literal.getTypeName() == SqlTypeName.SYMBOL) {
            final Enum<?> symbol = (Enum<?>) literal.getValue();
            return SqlLiteral.createSymbol(symbol, SqlImplementor.POS);
          }

          switch (literal.getTypeName().getFamily()) {
            case CHARACTER:
              return new SqlCharStringLiteralWithCollation(
                new NlsString((String) literal.getValue2(), null, null),
                SqlImplementor.POS,
                getDialect().getDefaultCollation(SqlKind.LITERAL));

            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
              return createIntervalLiteral(literal);

            case BINARY:
            case ANY:
            case NULL:
              switch (literal.getTypeName()) {
                case NULL:
                  return SqlLiteral.createNull(SqlImplementor.POS);

                default:
                  // Fall through.
              }

            default:
              // Fall through.
          }
          break;

        case FIELD_ACCESS:
          RexFieldAccess fieldAccess = (RexFieldAccess) rex;
          if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
            final RexFieldAccess access = (RexFieldAccess) rex;
            final Context outerContext = outerQueryAliasContextStack.peek();
            final SqlIdentifier sqlIdentifier =
              (SqlIdentifier) outerContext.field(access.getField().getIndex());
            return sqlIdentifier.skipLast(1).plus(fieldAccess.getField().getName(), SqlImplementor.POS);
          }
          return new SqlIdentifier(fieldAccess.getField().getName(), SqlImplementor.POS);
        case IN: {
          final RexSubQuery subQuery = (RexSubQuery) rex;
          outerQueryAliasContextStack.push(this);

          final Result subQueryResult = (Result) visitChild(0, subQuery.rel);

          outerQueryAliasContextStack.pop();

          ImmutableList.Builder<SqlNode> listBuilder = new ImmutableList.Builder<>();
          List<SqlNode> sqlNodes = toSql(program, subQuery.getOperands());
          if (1 == sqlNodes.size()) {
            listBuilder.addAll(sqlNodes);
          } else {
            listBuilder.add(new SqlNodeList(sqlNodes, POS));
          }

          if (subQuery.rel instanceof Project) {
            listBuilder.add(subQueryResult.asSelect());
          } else {
            listBuilder.add(subQueryResult.node);
          }

          return subQuery.getOperator().createCall(new SqlNodeList(listBuilder.build(), SqlImplementor.POS));
        }
        case SCALAR_QUERY:
        case EXISTS: {
          RexSubQuery subQuery = (RexSubQuery) rex;
          final SqlOperator subQueryOperator = subQuery.getOperator();
          outerQueryAliasContextStack.push(this);

          Result subQueryResult;
          if (subQuery.rel instanceof TableScan && rex.getKind() == SqlKind.EXISTS) {
            // When the child of an EXISTS is a table scan, it's not treated as a project so inject a project
            // to ensure that the SQL is generated correctly. Use a simple literal as this is only testing for
            // the presence or absence of rows, not the values of the rows.
            TableScan tableScan = (TableScan) subQuery.rel;
            final RexLiteral literalOne = tableScan.getCluster().getRexBuilder().makeBigintLiteral(BigDecimal.ONE);
            RelDataTypeFactory.FieldInfoBuilder builder = tableScan.getCluster().getTypeFactory().builder();
            builder.add("EXPR", literalOne.getType());

            Project project = ProjectRel.create(
              tableScan.getCluster(),
              tableScan.getTraitSet(),
              tableScan,
              ImmutableList.of(literalOne),
              builder.build());
            subQueryResult = (Result) visitChild(0, project);
          } else {
            subQueryResult = (Result) visitChild(0, subQuery.rel);
          }

          outerQueryAliasContextStack.pop();

          final List<SqlNode> operands = toSql(program, subQuery.getOperands());
          operands.add(subQueryResult.asNode());
          return subQueryOperator.createCall(new SqlNodeList(operands, SqlImplementor.POS));
        }

        case INPUT_REF: {
          SqlNode expr = super.toSql(program, rex);
          if (expr.getKind() == SqlKind.IDENTIFIER) {
            expr = addCastIfNeeded((SqlIdentifier) expr, rex.getType());
          }
          return expr;
        }

        default:
          // Fall through.
      }

      if (rex instanceof RexCall) {
        // Call back into DremioRelToSqlConverter to do a semantic transformation, if needed.
        final SqlNode callOverrideResult = getDremioRelToSqlConverter().convertCallToSql(this,
          program, (RexCall) rex, ignoreCast);
        if (callOverrideResult != null) {
          return callOverrideResult;
        }

        final RexCall originalCall = (RexCall) rex;
        final CallTransformer transformer = getDialect().getCallTransformer(originalCall);
        final Supplier<SqlNode> originalNodeSupplier = transformer.getAlternateCall(
          () -> super.toSql(program, rex), this, program, originalCall);
        return getDialect().decorateSqlNode(rex, originalNodeSupplier);
      }

      return super.toSql(program, rex);
    }

    /**
     * Transform an aggregate function to SQL. We provide the Aggregate node
     * for context about arguments.
     */
    public SqlNode toSql(AggregateCall aggCall, Aggregate aggregate) {
      return getDialect().decorateSqlNode(aggCall,
        () -> SqlTypeUtil.projectTypes(aggregate.getInput().getRowType(), aggCall.getArgList()),
        () -> super.toSql(aggCall));
    }

    @Override
    public List<SqlNode> toSql(RexProgram program, List<RexNode> rexNodes) {
      return super.toSql(program, rexNodes);
    }

    @Override
    public SqlCall toSql(RexProgram program, RexOver rexOver) {
      final RexWindow rexWindow = rexOver.getWindow();
      final SqlNodeList partitionList = new SqlNodeList(
        toSql(program, rexWindow.partitionKeys), POS);

      ImmutableList.Builder<SqlNode> orderNodes = ImmutableList.builder();
      if (rexWindow.orderKeys != null) {
        for (RexFieldCollation rfc : rexWindow.orderKeys) {
          // Omit ORDER BY <ordinal> clauses, which are parsed by Calcite but not actually
          // used for sorting.
          if (!(rfc.getKey() instanceof RexLiteral)) {
            if (rfc.getNullDirection() != dialect.defaultNullDirection(rfc.getDirection())) {
              // Get the SQL Node for the column being sorted on only.
              final SqlNode orderingColumnNode = toSql(program, rfc.left);

              final SqlNode emulatedNullDirNode = dialect.emulateNullDirection(orderingColumnNode,
                rfc.getNullDirection() == RelFieldCollation.NullDirection.FIRST, rfc.getDirection().isDescending());
              if (emulatedNullDirNode != null) {
                // Dialect requires emulating null direction.
                // Put the emulation in the order list first, then the ordering on the column only.
                orderNodes.add(emulatedNullDirNode);
                orderNodes.add(orderingColumnNode);
              } else {
                // Dialect implements NULLS FIRST and NULLS LAST clauses. These will get
                // unparsed as part of the RexFieldCollation.
                orderNodes.add(toSql(program, rfc));
              }
            } else {
              orderNodes.add(toSql(program, rfc));
            }
          }
        }
      }

      final SqlNodeList orderList =
        new SqlNodeList(orderNodes.build(), POS);

      final SqlLiteral isRows =
        SqlLiteral.createBoolean(rexWindow.isRows(), POS);

      final SqlNode lowerBound;
      final SqlNode upperBound;

      // Remove unnecessary Window Frames. When Calcite parses an OVER clause with no frame,
      final boolean hasUnnecessaryFrame = getDialect().removeDefaultWindowFrame(rexOver)
        && OverUtils.hasDefaultFrame(rexOver);

      if (hasUnnecessaryFrame) {
        lowerBound = null;
        upperBound = null;
      } else {
        lowerBound = createSqlWindowBound(rexWindow.getLowerBound());
        upperBound = createSqlWindowBound(rexWindow.getUpperBound());
      }

      // null defaults to true.
      // During parsing the allowPartial == false (e.g. disallow partial)
      // is expand into CASE expression and is handled as a such.
      // Not sure if we can collapse this CASE expression back into
      // "disallow partial" and set the allowPartial = false.
      final SqlLiteral allowPartial = null;

      final SqlWindow sqlWindow = getDremioRelToSqlConverter().adjustWindowForSource(
        this, rexOver.getAggOperator(), SqlWindow.create(
          null, null, partitionList,
          orderList, isRows, lowerBound, upperBound, allowPartial, POS));

      final List<SqlNode> nodeList = toSql(program, rexOver.getOperands());

      // Create the call for the aggregate in the window function.
      // If it happens to be a SUM0 call, we need to swap that with our own version which sets
      // the name to just SUM, rather than $SUM0, so that it can be translated to SQL compatible
      // with RDBMSes.
      final SqlAggFunction operator = rexOver.getAggOperator() != SqlStdOperatorTable.SUM0 ?
        rexOver.getAggOperator() : SUM0_FUNCTION;

      final SqlCall aggFunctionCall = operator.createCall(POS, nodeList);

      return SqlStdOperatorTable.OVER.createCall(POS, aggFunctionCall,
        sqlWindow);
    }

    /**
     * Searches this context
     *
     * @param fields The indices of the fields to search
     * @return
     */
    public boolean containsSqlCall(Iterable<Integer> fields) {
      for (int fieldIndex : fields) {
        final SqlNode field = field(fieldIndex);
        if (field instanceof SqlCall) {
          return true;
        }
      }
      return false;
    }

    /**
     * Searches this context
     *
     * @param fields The indices of the fields to search
     * @return
     */
    public boolean containsAggregateCall(Iterable<Integer> fields) {
      for (int fieldIndex : fields) {
        final SqlNode field = field(fieldIndex);
        if (field instanceof SqlCall
          && ((SqlCall) field).getOperator() instanceof SqlAggFunction) {
          return true;
        }
      }
      return false;
    }
  }

  protected class DremioAliasContext extends DremioContext {
    private final boolean qualified;
    private final Map<String, RelDataType> aliases;

    public DremioAliasContext(Map<String, RelDataType> aliases, boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.qualified = qualified;
    }

    @Override
    public SqlNode field(int ordinal) {
      for (Map.Entry<String, RelDataType> alias : getAliases().entrySet()) {
        if (ordinal < 0) {
          break;
        }

        final List<RelDataTypeField> fields = alias.getValue().getFieldList();

        if (ordinal < fields.size()) {
          RelDataTypeField field = fields.get(ordinal);
          final SqlNode mappedSqlNode = ordinalMap.get(field.getName().toLowerCase(Locale.ROOT));
          if (mappedSqlNode != null) {
            return mappedSqlNode;
          }

          // If getting a character column, add the DBMS-specific collation object representing Dremio's
          // collation sequence to it.
          final SqlCollation collation;
          if (field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && canAddCollation(field)) {
            collation = getDialect().getDefaultCollation(SqlKind.IDENTIFIER);
          } else {
            collation = null;
          }

          return new SqlIdentifier(!qualified && !getDremioRelToSqlConverter().isSubQuery() ?
            ImmutableList.of(field.getName()) :
            ImmutableList.of(alias.getKey(), field.getName()),
            collation,
            POS,
            null);
        }
        ordinal -= fields.size();
      }
      throw new AssertionError(
        "field ordinal " + ordinal + " out of range " + getAliases());
    }

    public Map<String, RelDataType> getAliases() {
      return aliases;
    }
  }

  protected class DremioJoinContext extends DremioContext {
    private final DremioContext leftContext;
    private final DremioContext rightContext;

    /**
     * Creates a JoinContext; use {@link #joinContext(Context, Context)}.
     */
    protected DremioJoinContext(Context leftContext, Context rightContext) {
      super(leftContext.fieldCount + rightContext.fieldCount);
      Preconditions.checkArgument(leftContext instanceof DremioContext);
      Preconditions.checkArgument(rightContext instanceof DremioContext);
      this.leftContext = (DremioContext) leftContext;
      this.rightContext = (DremioContext) rightContext;
    }

    @Override
    public SqlNode field(int ordinal) {
      if (ordinal < leftContext.fieldCount) {
        return leftContext.field(ordinal);
      } else {
        return rightContext.field(ordinal - leftContext.fieldCount);
      }
    }
  }

  /**
   * Context for translating MATCH_RECOGNIZE clause
   */
  private final class DremioMatchRecognizeContext extends DremioAliasContext {
    private DremioMatchRecognizeContext(Map<String, RelDataType> aliases) {
      super(aliases, false);
    }

    @Override
    public SqlNode toSql(RexProgram program, RexNode rex) {
      if (rex.getKind() == SqlKind.LITERAL) {
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          return new SqlIdentifier(RexLiteral.stringValue(literal), POS);
        }
      }
      return super.toSql(program, rex);
    }
  }

  /**
   * Creates a DremioSelectListContext; use {@link #selectListContext(SqlNodeList)}.
   */
  protected final class DremioSelectListContext extends DremioContext {
    private final SqlNodeList selectList;

    private DremioSelectListContext(SqlNodeList selectList) {
      super(selectList.size());
      this.selectList = selectList;
    }

    @Override
    public SqlNode field(int ordinal) {
      final SqlNode selectItem = selectList.get(ordinal);
      switch (selectItem.getKind()) {
        case AS:
          return ((SqlCall) selectItem).operand(0);

        default:
          // Fall through
      }
      return selectItem;
    }
  }

  // Builder and Result overrides

  /**
   * Dremio's custom implementation of SqlImplementor.Result
   */
  public class Result extends SqlImplementor.Result {

    public Result(SqlNode node, Collection<Clause> clauses, String neededAlias,
                  RelDataType neededType, Map<String, RelDataType> aliases) {
      super(node, clauses, neededAlias, neededType, aliases);
    }

    /**
     * Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     *
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.
     *
     * <p>When you have called
     * {@link Builder#setSelect(SqlNodeList)},
     * {@link Builder#setWhere(SqlNode)} etc. call
     * {@link Builder#result(SqlNode, Collection, RelNode, Map)}
     * to fix the new query.
     *
     * @param rel     Relational expression being implemented
     * @param clauses Clauses that will be generated to implement current
     *                relational expression
     * @return A builder
     */
    @Override
    public Builder builder(RelNode rel, Clause... clauses) {
      return builder(rel, false, clauses);
    }

    /**
     * Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     *
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.
     *
     * <p>When you have called
     * {@link Builder#setSelect(SqlNodeList)},
     * {@link Builder#setWhere(SqlNode)} etc. call
     * {@link Builder#result(SqlNode, Collection, RelNode, Map)}
     * to fix the new query.
     *
     * @param rel      Relational expression being implemented
     * @param forceNew Forces the query the result to be wrapped in a sub-query.
     * @param clauses  Clauses that will be generated to implement current
     *                 relational expression
     * @return A builder
     */
    public Builder builder(RelNode rel, boolean forceNew, Clause... clauses) {
      final Clause maxClause = maxClause();
      boolean needNew = forceNew;

      // If old and new clause are equal and belong to below set,
      // then new SELECT wrap is not required
      if (!needNew) {
        Set<Clause> nonWrapSet = ImmutableSet.of(Clause.SELECT);
        for (Clause clause : clauses) {
          if (maxClause.ordinal() > clause.ordinal() || (maxClause == clause && !nonWrapSet.contains(clause))) {
            needNew = true;
            break;
          }
        }
      }

      if (rel instanceof Project
        && ((Project) rel).getInput() instanceof Window
        && !windowStack.isEmpty()) {
        needNew = true;
      }

      if (rel instanceof LogicalAggregate
        && !dialect.supportsNestedAggregations()
        && hasNestedAggregations((LogicalAggregate) rel)) {
        needNew = true;
      }

      SqlSelect select;
      Expressions.FluentList<Clause> clauseList = Expressions.list();
      if (needNew) {
        select = subSelect();
      } else {
        select = asSelect();
        clauseList.addAll(this.clauses);
      }
      clauseList.appendAll(clauses);
      Context newContext;
      final SqlNodeList selectList = select.getSelectList();
      if (selectList != null) {
        newContext = getDremioRelToSqlConverter().selectListContext(selectList);
      } else {
        final boolean qualified = !dialect.hasImplicitTableAlias()
          || aliases.size() > 1 || getDremioRelToSqlConverter().isSubQuery();

        // basically, we did a subSelect() since needNew is set and neededAlias is not null
        // now, we need to make sure that we need to update the alias context.
        // if our aliases map has a single element:  <neededAlias, rowType>,
        // then we don't need to rewrite the alias but otherwise, it should be updated.
        if (needNew
          && neededAlias != null
          && (aliases.size() != 1 || !aliases.containsKey(neededAlias)
          || !aliases.get(neededAlias).equals(rel.getInput(0).getRowType()))) {
          final Map<String, RelDataType> newAliases =
            ImmutableMap.of(neededAlias, rel.getInput(0).getRowType());
          newContext = getDremioRelToSqlConverter().aliasContext(newAliases, qualified);
          return new Builder(rel, clauseList, select, newContext, rel.getRowType(), newAliases);
        } else {
          newContext = getDremioRelToSqlConverter().aliasContext(aliases, qualified);
        }
      }
      return new Builder(rel, clauseList, select, newContext, rel.getRowType(), aliases);
    }

    /**
     * Wraps a node in a SELECT statement.
     * "SELECT ... FROM (node)".
     * <p>
     * Additionally migrates the order by clause out of the nested query if appropriate.
     *
     * @param node SqlNode to be wrapped.
     * @return SqlSelect that represents the wrapped input node.
     */
    SqlSelect wrapSelectAndPushOrderBy(SqlNode node) {
      assert node instanceof SqlJoin
        || node instanceof SqlIdentifier
        || node instanceof SqlMatchRecognize
        || node instanceof SqlCall
        && (((SqlCall) node).getOperator() instanceof SqlSetOperator
        || ((SqlCall) node).getOperator() == SqlStdOperatorTable.AS
        || ((SqlCall) node).getOperator() == SqlStdOperatorTable.VALUES)
        : node;

      // Migrate the order by clause to the wrapping select statement,
      // if there is no fetch or offset clause accompanying the order by clause.
      SqlNodeList pushUpOrderList = null;
      if ((this.node instanceof SqlSelect) && shouldPushOrderByOut()) {
        SqlSelect selectClause = (SqlSelect) this.node;
        if ((selectClause.getFetch() == null) && (selectClause.getOffset() == null)) {
          pushUpOrderList = selectClause.getOrderList();
          selectClause.setOrderBy(null);

          if (node.getKind() == SqlKind.AS && pushUpOrderList != null) {
            // Update the referenced tables of the ORDER BY list to use the alias of the sub-select.

            List<SqlNode> selectList = (selectClause.getSelectList() == null)?
              Collections.emptyList() : selectClause.getSelectList().getList();

            OrderByAliasProcessor processor = new OrderByAliasProcessor(
              pushUpOrderList, SqlValidatorUtil.getAlias(node, -1), selectList);
            pushUpOrderList = processor.processOrderBy();
          }
        }
      }

      SqlNodeList selectList = getSelectList(node);

      return new SqlSelect(POS, SqlNodeList.EMPTY, selectList, node, null, null, null,
        SqlNodeList.EMPTY, pushUpOrderList, null, null);
    }

    protected SqlNodeList getSelectList(SqlNode node) {
      SqlNodeList selectList = null;
      if (node instanceof SqlIdentifier && (this.neededType.getFieldCount() != 0)) {
        // Ensure the query isn't changed into a SELECT * to preserve the field ordering.
        selectList = new SqlNodeList(POS);
        final String nodeName = ((SqlIdentifier)node).names.reverse().iterator().next();
        for (RelDataTypeField field : this.neededType.getFieldList()) {
          final SqlCollation collation;

          if (canAddCollation(field)) {
            collation = getDialect().getDefaultCollation(SqlKind.IDENTIFIER);
          } else {
            collation = null;
          }

          final List<String> names = new ArrayList<>();
          names.add(nodeName);
          names.add(field.getName());

          if (null != collation) {
            final SqlIdentifier ident = new SqlIdentifier(
              names,
              collation,
              POS,
              null);
            selectList.add(SqlStdOperatorTable.AS.createCall(
              POS, addCastIfNeeded(ident, field.getType()), new SqlIdentifier(field.getName(), POS)));
          } else {
            final SqlIdentifier ident = new SqlIdentifier(names, POS);
            SqlNode possibleCast = addCastIfNeeded(ident, field.getType());
            if (ident != possibleCast) {
              // Only add an alias here if a cast was added, to maintain the schema.
              possibleCast = SqlStdOperatorTable.AS.createCall(
                POS, possibleCast, new SqlIdentifier(field.getName(), POS));
            }
            selectList.add(possibleCast);
          }
        }
      } else if (node instanceof SqlJoin && this.neededType == null) {
        final Set<String> usedNames = new HashSet<>();
        // Avoid * where possible when used with JOINs, as it can lead to ambiguous column references.
        final List<SqlNode> nodeList = addJoinChildSelectNodes(node, usedNames);
        if (nodeList != null) {
          selectList = new SqlNodeList(nodeList, POS);
        }
      }

      return selectList;
    }

    private List<SqlNode> addJoinChildSelectNodes(SqlNode node, Set<String> usedNames) {
      final SqlNode childNode = (node.getKind() == SqlKind.AS) ? ((SqlBasicCall) node).getOperands()[0] : node;

      if (childNode.getKind() == SqlKind.JOIN) {
        final SqlJoin join = (SqlJoin)childNode;
        // Delegate to the children of the join to get the node list.
        final List<SqlNode> leftList = addJoinChildSelectNodes(join.getLeft(), usedNames);
        final List<SqlNode> rightList = addJoinChildSelectNodes(join.getRight(), usedNames);
        if (leftList == null || rightList == null) {
          // Not possible to get the nodes of one or the other child, abandon the effort.
          return null;
        }

        return ImmutableList.<SqlNode>builder().addAll(leftList).addAll(rightList).build();
      }

      if (childNode.getKind() != SqlKind.SELECT || ((SqlSelect)childNode).getSelectList() == null) {
        return null;
      }

      // Although we are expanding the * into a list of column references, don't do anything but the expansion as
      // the underlying references will have any necessary modifications (ie addition of explicit casts) done to them
      // already.
      final String tableAlias = SqlValidatorUtil.getAlias(node, -1);
      final List<SqlNode> selectList = new ArrayList<>();
      ((SqlSelect) childNode).getSelectList().getList().forEach(n -> {
        String colAlias = SqlValidatorUtil.getAlias(n, -1);
        if (null == colAlias) {
          // Guard against possible null aliases being returned by generating a unique value.
          colAlias = SqlValidatorUtil.uniquify(colAlias, usedNames, SqlValidatorUtil.EXPR_SUGGESTER);
        } else if (colAlias.equals("\"*\"")) {
          // If * is used as an alias, it ends up getting double quoted when it should not be.
          colAlias = "*";
        }
        final List<String> names = (tableAlias != null) ? ImmutableList.of(tableAlias, colAlias) : ImmutableList.of(colAlias);

        if (n.getKind() == SqlKind.IDENTIFIER) {
          // Ensure we have unique names being used.
          final String alias = SqlValidatorUtil.uniquify(colAlias, usedNames, SqlValidatorUtil.EXPR_SUGGESTER);

          selectList.add(SqlStdOperatorTable.AS.createCall(
            POS,
            new SqlIdentifier(names, n.getParserPosition()),
            new SqlIdentifier(alias, POS)));
        } else if (n.getKind() == SqlKind.AS) {
          selectList.add(new SqlIdentifier(names, POS));
        } else {
          selectList.add(n);
        }

        usedNames.add(colAlias);
      });

      return selectList;
    }

    @Override
    public SqlSelect subSelect() {
      return wrapSelectAndPushOrderBy(asFrom());
    }

    /**
     * Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is.
     */
    @Override
    public SqlSelect asSelect() {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      }
      if (node.isA(SqlKind.SET_QUERY) || isSubQuery() || !dialect.hasImplicitTableAlias()) {
        return wrapSelectAndPushOrderBy(asFrom());
      }
      return wrapSelectAndPushOrderBy(node);
    }

    /**
     * Return the SqlNode as is.
     */
    public SqlNode asNode() {
      return node;
    }
  }

  /**
   * Determine whether or not the given field can have a collation added to it.
   * @param field
   * @return
   */
  protected boolean canAddCollation(RelDataTypeField field) {
    return field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
  }

  protected SqlNode addCastIfNeeded(SqlIdentifier expr, RelDataType type) {
    return expr;
  }

  protected static boolean isApproximateNumeric(RelDataType type) {
    return type.getSqlTypeName().equals(SqlTypeName.DOUBLE) ||
      type.getSqlTypeName().equals(SqlTypeName.FLOAT) ||
      type.getSqlTypeName().equals(SqlTypeName.REAL);
  }

  protected static boolean isDecimal(RelDataType type) {
    return type.getSqlTypeName().equals(SqlTypeName.DECIMAL);
  }

  /**
   * Dremio's custom implementation of SqlImplementor.Builder
   */
  public class Builder extends SqlImplementor.Builder {
    private final RelDataType needType;

    public Builder(RelNode rel, List<Clause> clauses, SqlSelect select,
                   Context context, RelDataType needType, Map<String, RelDataType> aliases) {
      super(rel, clauses, select, context, aliases);
      this.needType = needType;
    }

    @Override
    public void addOrderItem(List<SqlNode> orderByList,
                             RelFieldCollation field) {
      // Do not attempt to order by literals as databases such as SQL Server
      // disallow ORDER BY on const expressions.
      if (!(context.toSql(field) instanceof SqlLiteral)) {
        super.addOrderItem(orderByList, field);
      }
    }

    @Override
    public Result result() {
      return getDremioRelToSqlConverter().result(select, clauses, rel, getNeedType(), aliases);
    }

    public RelDataType getNeedType() {
      return needType;
    }

    public Map<String, RelDataType> getAliases() {
      return aliases;
    }
  }
}
