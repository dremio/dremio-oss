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
package com.dremio.exec.planner.sql.handlers;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.RelDataTypeSystemImpl;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * A bottom up visitor to rewrite a Join if there is a range join condition
 */
public class RangeConditionRewriteVisitor extends StatelessRelShuttleImpl {
  private static final int INTERVALS = 100; // Total intervals
  private static final int INTERVALS_SQRT = (int) Math.pow(INTERVALS, 0.5);

  private final PlannerSettings plannerSettings;

  /**
   * A rangeConditionInfo is used to see if there's a between operator in the Logical join.
   * If there exists a between operator, eg: a between b and c, which is also a >= b and a <= c
   * The logical plan is AND(>=($1, $3), <=($1, $4)) implies $1 = a, $3 = b, $4 = c
   */
  private RangeConditionInfo rangeConditionInfo = new RangeConditionInfo();
  private Pair<Double, Double> range = null;

  public RangeConditionRewriteVisitor(PlannerSettings plannerSettings) {
    this.plannerSettings = plannerSettings;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    super.visit(join);
    if (!checkPreconditions(join)) {
      return join;
    }
    validateRangeConditionInfo();
    int middleIndex = rangeConditionInfo.mid;
    int leftFieldSize = join.getLeft().getRowType().getFieldCount();
    int rightFieldSize = join.getRight().getRowType().getFieldCount();

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode indexTable = buildIndexTable(join, rexBuilder, range, middleIndex);

    if (middleIndex < leftFieldSize) {
      int leftIndex = rangeConditionInfo.left - leftFieldSize;
      int rightIndex = rangeConditionInfo.right - leftFieldSize;
      RelNode updatedLeft = joinLeftTableWithIndex(join.getLeft(), indexTable, rexBuilder, middleIndex);
      RelNode updatedRight = joinRightTableWithIndex(join.getRight(), indexTable, rexBuilder, leftIndex, rightIndex);
      RelNode finalJoin = joinAndProject(updatedLeft, updatedRight, rexBuilder, leftIndex, rightIndex, middleIndex);
      RelNode finalProject = applyLeftProject(finalJoin, rexBuilder, leftFieldSize);
      return finalProject;
    } else {
      int leftIndex = rangeConditionInfo.left;
      int rightIndex = rangeConditionInfo.right;
      middleIndex = middleIndex - leftFieldSize;
      RelNode updatedLeft = joinLeftTableWithIndex(join.getRight(), indexTable, rexBuilder, middleIndex);
      RelNode updatedRight = joinRightTableWithIndex(join.getLeft(), indexTable, rexBuilder, leftIndex, rightIndex);
      RelNode finalJoin = joinAndProject(updatedLeft, updatedRight, rexBuilder, leftIndex, rightIndex, middleIndex);
      RelNode finalProject = applyRightProject(finalJoin, rexBuilder, rightFieldSize, leftFieldSize);
      return finalProject;
    }
  }

  private void validateRangeConditionInfo() {
    Preconditions.checkArgument(!rangeConditionInfo.isEmpty, "RangeConditionInfo is not initialized");
  }

  private RelNode applyRightProject(RelNode logicalJoin, RexBuilder rexBuilder, int rightFieldSize, int leftFieldSize) {
    List<String> fieldNames = new ArrayList<>();
    List<RexNode> temp = new ArrayList<>();
    for (int i = rightFieldSize + 1; i < rightFieldSize + 1 + leftFieldSize; i++) {
      fieldNames.add(logicalJoin.getRowType().getFieldNames().get(i));
      temp.add(rexBuilder.makeInputRef(logicalJoin, i));
    }
    for (int i = 0; i < rightFieldSize; i++) {
      fieldNames.add(logicalJoin.getRowType().getFieldNames().get(i));
      temp.add(rexBuilder.makeInputRef(logicalJoin, i));
    }
    List<RexNode> projects = ImmutableList.copyOf(temp);
    final LogicalProject finalProject = LogicalProject.create(
      logicalJoin,
      ImmutableList.of(),
      projects,
      fieldNames
    );
    return finalProject;
  }

  private RelNode applyLeftProject(RelNode logicalJoin, RexBuilder rexBuilder, int leftFieldSize) {
    List<String> fieldNames = new ArrayList<>(logicalJoin.getRowType().getFieldNames());
    fieldNames.remove(leftFieldSize);
    fieldNames.remove(fieldNames.size() - 1);
    List<RexNode> temp = new ArrayList<>();
    for (int i = 0; i < logicalJoin.getRowType().getFieldCount(); i++) {
      temp.add(rexBuilder.makeInputRef(logicalJoin, i));
    }
    temp.remove(leftFieldSize);
    temp.remove(temp.size() - 1);
    List<RexNode> projects = ImmutableList.copyOf(temp);
    final LogicalProject finalProject = LogicalProject.create(
      logicalJoin,
      ImmutableList.of(),
      projects,
      fieldNames
    );
    return finalProject;
  }

  /**
   * Matching Condition is to have an inequality join and join condition is on both sides
   * scan all the operands and see if there is at least one operand from opposite side on each side of the join condition.
   */
  private boolean checkPreconditions(LogicalJoin join) {
    if (!plannerSettings.getOptions().getOption(PlannerSettings.ENABLE_RANGE_QUERY_REWRITE)) {
      return false;
    }

    RexNode condition = join.getCondition();
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator() == SqlStdOperatorTable.AND && call.getOperands().size() == 2) {
        RangeConditionInfoVisitor visitor = new RangeConditionInfoVisitor(join);
        join.getCondition().accept(visitor);
        rangeConditionInfo = visitor.getRangeConditionInfo();
        return visitor.hasBetweenOperator() && statsAvailable(join);
      }
    }

    return false;
  }

  private boolean statsAvailable(LogicalJoin join) {
    validateRangeConditionInfo();
    StatisticsService statisticsService = plannerSettings.getStatisticsService();
    FullPathRelNodeVisitor visitor = new FullPathRelNodeVisitor(rangeConditionInfo.left);
    join.accept(visitor);
    List<String> tablePath = visitor.getFullPath();

    RelDataType rowType;
    int leftIndex;
    int rightIndex;

    int leftFieldSize = join.getLeft().getRowType().getFieldCount();
    if (rangeConditionInfo.mid < leftFieldSize) {
      // The between operator is like "someCol from left table is between 2 columns from right table
      leftIndex = rangeConditionInfo.left - leftFieldSize;
      rightIndex = rangeConditionInfo.right - leftFieldSize;
      rowType = join.getRight().getRowType();
    } else {
      // The between operator is like "someCol from right table is between 2 columns from left table
      leftIndex = rangeConditionInfo.left;
      rightIndex = rangeConditionInfo.right;
      rowType = join.getLeft().getRowType();
    }
    String leftColumnName = rowType.getFieldNames().get(leftIndex);
    String rightColumnName = rowType.getFieldNames().get(rightIndex);
    SqlTypeName leftColumnType = rowType.getFieldList().get(leftIndex)
      .getType().getSqlTypeName();
    SqlTypeName rightColumnType = rowType.getFieldList().get(rightIndex)
      .getType().getSqlTypeName();

    StatisticsService.Histogram leftHistogram = statisticsService.getHistogram(
      leftColumnName, new NamespaceKey(tablePath), leftColumnType);
    StatisticsService.Histogram rightHistogram = statisticsService.getHistogram(
      rightColumnName, new NamespaceKey(tablePath), rightColumnType);
    if (leftHistogram == null || rightHistogram == null) {
      return false;
    }
    range = Pair.of(Math.min(leftHistogram.quantile(0), rightHistogram.quantile(0)),
      Math.max(leftHistogram.quantile(1), rightHistogram.quantile(1)));
    return true;
  }


  private RelNode joinAndProject(RelNode updatedLeft, RelNode updatedRight, RexBuilder rexBuilder,
                                 int leftIndex, int rightIndex, int middleIndex) {
    final int leftFieldCount = updatedLeft.getRowType().getFieldCount();
    final int rightFieldCount = updatedRight.getRowType().getFieldCount();
    final RexNode greaterOrEqualCond = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(updatedLeft, middleIndex),
      rexBuilder.makeInputRef(updatedRight.getRowType().getFieldList().get(leftIndex).getType(), leftIndex + leftFieldCount));

    final RexNode lessOrEqualCond = rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(updatedLeft, middleIndex),
      rexBuilder.makeInputRef(updatedRight.getRowType().getFieldList().get(rightIndex).getType(), rightIndex + leftFieldCount));

    final RexNode equalsCond = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(updatedLeft, leftFieldCount - 1),
      rexBuilder.makeInputRef(updatedRight.getRowType().getFieldList().get(rightFieldCount - 1).getType(), rightFieldCount + leftFieldCount - 1));

    final RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, greaterOrEqualCond, lessOrEqualCond, equalsCond);
    final LogicalJoin logicalJoin = LogicalJoin.create(updatedLeft, updatedRight, ImmutableList.of(), condition, ImmutableSet.of(), JoinRelType.INNER);
    return logicalJoin;
  }

  private RelNode joinRightTableWithIndex(RelNode right, RelNode indexTable,
                                          RexBuilder rexBuilder,
                                          int leftIndex, int rightIndex) {
    final RelDataType decimal20x1 = rexBuilder.getTypeFactory().createSqlType(DECIMAL, 20, 1);
    final int rightFieldCount = right.getRowType().getFieldCount();
    final RexNode greaterCase = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      rexBuilder.makeInputRef(right, leftIndex),
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(1).getType(), 1 + rightFieldCount));

    final RexNode cast1 = rexBuilder.makeCast(decimal20x1, rexBuilder.makeInputRef(right, leftIndex), true);
    final RexNode cast4 = rexBuilder.makeCast(
      decimal20x1,
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(1).getType(), 1 + rightFieldCount),
      true);

    final RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.CASE, greaterCase, cast1, cast4);
    final RexNode lessCase = rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN,
      rexBuilder.makeInputRef(right, rightIndex),
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(2).getType(), 2 + rightFieldCount));

    final RexNode cast2 = rexBuilder.makeCast(decimal20x1, rexBuilder.makeInputRef(right, rightIndex), true);
    final RexNode cast5 = rexBuilder.makeCast(
      decimal20x1,
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(2).getType(), 2 + rightFieldCount),
      true);
    final RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.CASE, lessCase, cast2, cast5);
    final RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, cond1, cond2);
    LogicalJoin logicalJoin = LogicalJoin.create(right, indexTable, ImmutableList.of(), condition, ImmutableSet.of(), JoinRelType.LEFT);

    List<RexNode> projects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(right.getRowType().getFieldNames());
    for (int i = 0; i < rightFieldCount; i++) {
      projects.add(rexBuilder.makeInputRef(logicalJoin, i));
    }
    projects.add(rexBuilder.makeInputRef(logicalJoin, rightFieldCount));
    fieldNames.add("d_id");
    final LogicalProject logicalProject = LogicalProject.create(
      logicalJoin,
      ImmutableList.of(),
      projects,
      fieldNames);
    return logicalProject;
  }

  /**
   * Project the logicalJoin RelNode into the
   */
  private RelNode joinLeftTableWithIndex(RelNode left, RelNode indexTable, RexBuilder rexBuilder, int middleIndex) {
    final int leftFieldCount = left.getRowType().getFieldCount();
    final RexNode greaterOrEqualCond = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(left, middleIndex),
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(1).getType(), 1 + leftFieldCount));

    final RexNode lessCond = rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN,
      rexBuilder.makeInputRef(left, middleIndex),
      rexBuilder.makeInputRef(indexTable.getRowType().getFieldList().get(2).getType(), 2 + leftFieldCount));
    final RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, greaterOrEqualCond, lessCond);

    final LogicalJoin logicalJoin = LogicalJoin.create(left, indexTable, ImmutableList.of(), condition, ImmutableSet.of(), JoinRelType.LEFT);
    List<RexNode> temp = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(left.getRowType().getFieldNames());
    for (int i = 0; i < leftFieldCount; i++) {
      temp.add(rexBuilder.makeInputRef(logicalJoin, i));
    }
    temp.add(rexBuilder.makeInputRef(logicalJoin, leftFieldCount));
    fieldNames.add("d_id");
    List<RexNode> projects = ImmutableList.copyOf(temp);
    LogicalProject logicalProject = LogicalProject.create(
      logicalJoin,
      ImmutableList.of(),
      projects,
      fieldNames);
    return logicalProject;
  }

  /**
   * Check whether a sql contains a between operator
   */
  private static class RangeConditionInfoVisitor extends RexVisitorImpl<Void> {
    private final int leftFields;
    private final int[] lessOrEqualArray;
    private final List<RangeConditionInfo> rangeConditionInfoList;
    private boolean hasBetweenOperator;

    public RangeConditionInfoVisitor(LogicalJoin join) {
      super(true);
      this.leftFields = join.getLeft().getRowType().getFieldCount();
      this.lessOrEqualArray = new int[join.getRowType().getFieldCount() + 1];
      this.rangeConditionInfoList = new ArrayList<>();
      Arrays.fill(lessOrEqualArray, -1);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (call.getOperator().getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
        if (call.getOperands().get(0) instanceof RexInputRef && call.getOperands().get(1) instanceof RexInputRef) {
          int left = ((RexInputRef) call.getOperands().get(0)).getIndex();
          int right = ((RexInputRef) call.getOperands().get(1)).getIndex();
          lessOrEqualArray[right] = left;
        }
      } else if (call.getOperator().getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
        if (call.getOperands().get(0) instanceof RexInputRef && call.getOperands().get(1) instanceof RexInputRef) {
          int left = ((RexInputRef) call.getOperands().get(0)).getIndex();
          int right = ((RexInputRef) call.getOperands().get(1)).getIndex();
          lessOrEqualArray[left] = right;
        }
      } else if (call.getOperator().getKind() == SqlKind.AND) {
        for (RexNode operand : call.operands) {
          operand.accept(this);
        }
      }
      return null;
    }

    public RangeConditionInfo getRangeConditionInfo() {
      for (int i = 0; i < lessOrEqualArray.length; i++) {
        if (lessOrEqualArray[i] != -1) {
          int middleVal = lessOrEqualArray[i];
          if (lessOrEqualArray[middleVal] != -1) {
            int left = i;
            int mid = middleVal;
            int right = lessOrEqualArray[middleVal];

            // If the middle field is from left side of the join, make sure the other fields are
            // from right side of the join, or vice versa.
            boolean rangeConditionSupported = (mid < leftFields && left >= leftFields && right >= leftFields) ||
              (mid >= leftFields && left < leftFields && right < leftFields);
            if (rangeConditionSupported) {
              rangeConditionInfoList.add(new RangeConditionInfo(left, mid, right));
              hasBetweenOperator = true;
            }
          }
        }
      }
      return rangeConditionInfoList.size() == 0 ? new RangeConditionInfo() :
        rangeConditionInfoList.get(0); // At the moment, we are assuming that there is only 1 between condition
    }

    public boolean hasBetweenOperator() {
      return hasBetweenOperator;
    }
  }

  /**
   * A visitor to find full path for the table with given column field
   */
  private static class FullPathRelNodeVisitor extends StatelessRelShuttleImpl {

    private int fieldIndex;
    private List<String> fullPath;

    public FullPathRelNodeVisitor(int fieldIndex) {
      this.fieldIndex = fieldIndex;
      fullPath = new ArrayList<>();
    }

    @Override
    public RelNode visit(TableScan scan) {
      if (scan instanceof ScanRelBase) {
        ScanRelBase scanRelBase = (ScanRelBase) scan;
        fullPath = scanRelBase.getTableMetadata().getDatasetConfig().getFullPathList();
      }
      return super.visit(scan);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (child instanceof LogicalProject) {
        LogicalProject project = (LogicalProject) child;
        RelDataType rowType = parent.getRowType();
        String fieldName = rowType.getFieldNames().get(fieldIndex);
        Pair<Integer, RelDataTypeField> pair = MoreRelOptUtil.findFieldWithIndex(project.getRowType().getFieldList(),
          rowType.getFieldNames().get(fieldIndex));
        Preconditions.checkArgument(pair != null,
          String.format("Can not find field: %s in row: %s", fieldName, rowType.toString()));
        fieldIndex = pair.left;
      }

      RelNode child2 = child.accept(this);
      if (child2 != child) {
        final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
        newInputs.set(i, child2);
        return parent.copy(parent.getTraitSet(), newInputs);
      }
      return parent;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      if (fieldIndex < join.getLeft().getRowType().getFieldCount()) {
        return visitChild(join, 0, join.getLeft());
      } else {
        fieldIndex += join.getLeft().getRowType().getFieldCount();
        return visitChild(join, 1, join.getRight());
      }
    }

    public List<String> getFullPath() {
      return fullPath;
    }
  }

  /**
   * Build a Table based on the start and end value
   */
  private RelNode buildIndexTable(LogicalJoin join,
                                  RexBuilder rexBuilder,
                                  Pair<Double, Double> range,
                                  int middleIndex) {
    // Build RowType & Tuples
    RelDataTypeField relDataType = new RelDataTypeFieldImpl(
      "ROW_VALUE",
      0,
      new BasicSqlType(RelDataTypeSystemImpl.REL_DATA_TYPE_SYSTEM, SqlTypeName.ANY));
    RelDataType rowType = new RelRecordType(StructKind.FULLY_QUALIFIED, Arrays.asList(relDataType));
    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = new ImmutableList.Builder<>();
    for (int i = 0; i < INTERVALS_SQRT; i++) {
      tuples.add(new ImmutableList.Builder<RexLiteral>().add(
        rexBuilder.makeExactLiteral(new BigDecimal(i))).build());
    }

    // Construct ValuesRel
    final RelTraitSet traits = RelTraitSet.createEmpty()
      .plus(Convention.NONE)
      .plus(DistributionTrait.DEFAULT)
      .plus(RelCollations.EMPTY);

    final LogicalValues logicalValues = new LogicalValues(join.getCluster(), traits, rowType, tuples.build());
    LogicalProject logicalProject = LogicalProject.create(
      logicalValues,
      ImmutableList.of(),
      rexBuilder.identityProjects(logicalValues.getRowType()),
      Arrays.asList("a"));

    LogicalJoin logicalJoin = LogicalJoin.create(
      logicalProject,
      logicalProject,
      ImmutableList.of(),
      rexBuilder.makeLiteral(true),
      ImmutableSet.of(),
      JoinRelType.INNER);

    final RexNode input = rexBuilder.makeInputRef(logicalJoin.getLeft(), 0);
    final RexNode multiNode = rexBuilder.makeCall(
      SqlStdOperatorTable.MULTIPLY,
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(INTERVALS_SQRT)),
      input);
    final RexNode addNode = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, multiNode, rexBuilder.makeInputRef(logicalJoin, 1));
    final LogicalProject project1 = LogicalProject.create(
      logicalJoin,
      ImmutableList.of(),
      ImmutableList.of(addNode),
      Arrays.asList("val"));

    double intervalWidth = (range.right - range.left) / INTERVALS;

    final RexNode zeroInputRef = rexBuilder.makeInputRef(project1, 0);
    final RexNode multiNode2 = getStartValueNode(join.getRowType().getFieldList().get(middleIndex).getType(), rexBuilder, intervalWidth, zeroInputRef);

    LogicalProject project2 = LogicalProject.create(
      project1,
      ImmutableList.of(),
      ImmutableList.of(zeroInputRef, multiNode2),
      Arrays.asList("val", "start_value"));

    final RexNode secondInputRef = rexBuilder.makeInputRef(project2, 1);
    final RexNode minusOne = rexBuilder.makeCall(
      SqlStdOperatorTable.MINUS,
      zeroInputRef,
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)));
    final LogicalProject project3 = LogicalProject.create(
      project2,
      ImmutableList.of(),
      ImmutableList.of(zeroInputRef, secondInputRef, minusOne),
      Arrays.asList("val", "start_value", "end_value"));

    final RexNode condition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      zeroInputRef,
      rexBuilder.makeInputRef(project3.getRowType().getFieldList().get(2).getType(), 2 + project2.getRowType().getFieldCount()));
    LogicalJoin logicalJoin1 = LogicalJoin.create(project2, project3, ImmutableList.of(), condition, ImmutableSet.of(), JoinRelType.FULL);

    List<RexNode> projects = ImmutableList.of(
      rexBuilder.makeInputRef(logicalJoin1, 0),
      rexBuilder.makeInputRef(logicalJoin1, 1),
      rexBuilder.makeInputRef(logicalJoin1, 2),
      rexBuilder.makeInputRef(logicalJoin1, 3));
    final LogicalProject project4 = LogicalProject.create(
      logicalJoin1,
      ImmutableList.of(),
      projects,
      Arrays.asList("val", "start_value", "val0", "start_value0"));

    RexNode rowNumCond = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, zeroInputRef),
      zeroInputRef,
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(-1)));

    RexNode startValCond = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, secondInputRef),
      secondInputRef,
      getLowerBound(join.getRowType().getFieldList().get(middleIndex).getType(), rexBuilder));
    RexNode forthInputRef = rexBuilder.makeInputRef(project4, 3);

    RexNode endValCond = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, forthInputRef),
      forthInputRef,
      getUpperBound(join.getRowType().getFieldList().get(middleIndex).getType(), rexBuilder));
    final LogicalProject project5 = LogicalProject.create(
      project4,
      ImmutableList.of(),
      ImmutableList.of(rowNumCond, startValCond, endValCond),
      Arrays.asList("_id", "start_value", "end_value"));

    RelFieldCollation collation = new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST);
    final LogicalSort logicalSort = LogicalSort.create(project5, RelCollations.of(collation), null, null);
    return logicalSort;
  }

  @SuppressWarnings("FallThrough") // FIXME: remove suppression by properly handling switch fallthrough
  private RexNode getStartValueNode(RelDataType type, RexBuilder rexBuilder, double intervalWidth, RexNode zeroInputRef) {
    switch (type.getSqlTypeName().getFamily()) {
      case NUMERIC:
        return rexBuilder.makeCall(
          SqlStdOperatorTable.MULTIPLY,
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(intervalWidth)),
          zeroInputRef);
      case DATE: // TODO
        rexBuilder.makeCall(
          SqlStdOperatorTable.DATETIME_PLUS,
          rexBuilder.makeDateLiteral(new DateString("1992-01-01")),
          rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(intervalWidth)),
            zeroInputRef));
      case TIME: // TODO
      case TIMESTAMP: // TODO
      default:
        throw new RuntimeException("Unsupported type: " + type);
    }
  }

  private RexNode getLowerBound(RelDataType type, RexBuilder rexBuilder) {
    switch (type.getSqlTypeName()) {
      case INTEGER:
      case BIGINT:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(Integer.MIN_VALUE));
      case DATE:
        return rexBuilder.makeDateLiteral(new DateString("1970-01-01"));
      case TIME:
        return rexBuilder.makeTimeLiteral(new TimeString("00:00:00"), type.getPrecision());
      case TIMESTAMP:
        return rexBuilder.makeTimestampLiteral(new TimestampString("1970-01-01 00:00:00"), type.getPrecision());
      default:
        throw new RuntimeException(String.format("Unsupported type: %s", type.toString()));
    }
  }

  private RexNode getUpperBound(RelDataType type, RexBuilder rexBuilder) {
    switch (type.getSqlTypeName()) {
      case INTEGER:
      case BIGINT:
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(Integer.MAX_VALUE));
      case DATE:
        return rexBuilder.makeDateLiteral(new DateString("2038-12-31"));
      case TIME:
        return rexBuilder.makeTimeLiteral(new TimeString("23:59:59"), type.getPrecision());
      case TIMESTAMP:
        return rexBuilder.makeTimestampLiteral(new TimestampString("2038-12-31 00:00:00"), type.getPrecision());
      default:
        throw new RuntimeException(String.format("Unsupported type: %s", type.toString()));
    }
  }

  private static class RangeConditionInfo {
    private final int left;
    private final int mid;
    private final int right;
    private final boolean isEmpty;

    public RangeConditionInfo() {
      this(-1, -1, -1, true);
    }

    public RangeConditionInfo(int left, int mid, int right) {
      this(left, mid, right, false);
    }

    private RangeConditionInfo(int left, int mid, int right, boolean isEmpty) {
      this.left = left;
      this.mid = mid;
      this.right = right;
      this.isEmpty = isEmpty;
    }
  }
}
