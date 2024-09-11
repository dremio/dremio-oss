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
package com.dremio.exec.planner.physical;

import static com.dremio.exec.util.ColumnUtils.isSystemColumn;
import static org.apache.calcite.rel.core.TableModify.Operation.MERGE;
import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.TableManagementPlanGenerator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections4.CollectionUtils;

/***
 * Generate a DML query plan. Designed for Delete, Update and Merge operations.
 * DML plan can generate an iceberg 'merge_on_read' plan or 'copy_on_write' plan
 */
public abstract class DmlPlanGeneratorBase extends TableManagementPlanGenerator {

  protected final TableModify.Operation operation;
  // update column names along with its index
  protected final Map<String, Integer> updateColumnsWithIndex = new HashMap<>();
  // If the TableModify operation has a source
  protected final boolean hasSource;

  protected final int tableColumnCount;

  protected final int inputColumnCount;

  protected int updateColumnCount;

  protected int insertColumnCount;

  // Merge-On-Read only (for now).
  protected int parsedOutdatedTargetColumns = 0;

  // Merge-On-Read only (for now).
  protected Set<String> outdatedTargetColumnNames;

  public enum MergeType {
    UPDATE_ONLY,
    UPDATE_INSERT,
    INSERT_ONLY,
    INVALID
  }

  protected MergeType mergeType = MergeType.INVALID;

  public DmlPlanGeneratorBase(
      RelOptTable table,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      TableMetadata tableMetadata,
      CreateTableEntry createTableEntry,
      TableModify.Operation operation,
      List<String> updateColumnList,
      boolean hasSource,
      OptimizerRulesContext context) {
    super(table, cluster, traitSet, input, tableMetadata, createTableEntry, context);
    this.operation = Preconditions.checkNotNull(operation, "DML operation cannot be null.");
    this.hasSource = hasSource;
    this.inputColumnCount = input.getRowType().getFieldCount();
    this.tableColumnCount = table.getRowType().getFieldCount();
    this.updateColumnCount = updateColumnList != null ? updateColumnList.size() : 0;
  }

  protected void validateOperationType(
      TableModify.Operation operation, List<String> updateColumnList) {
    // validate updateColumnList
    if (operation == UPDATE || operation == TableModify.Operation.MERGE) {
      if (CollectionUtils.isNotEmpty(updateColumnList)) {
        for (int i = 0; i < updateColumnList.size(); i++) {
          this.updateColumnsWithIndex.put(updateColumnList.get(i), i);
        }
      }
      validateMergeTypeIfExist();
    }
  }

  /** validates the merge type */
  public void validateMergeTypeIfExist() {
    // set and validate merge type
    if (this.operation == MERGE) {
      if (updateColumnCount == 0) {
        mergeType = MergeType.INSERT_ONLY;
      } else if (insertColumnCount == 0) {
        mergeType = MergeType.UPDATE_ONLY;
      } else {
        mergeType = MergeType.UPDATE_INSERT;
      }
    }
  }

  /**
   * Get the Copy-On-Write or Merge-On-Read Physical Plan.
   *
   * @return the dml physical plan.
   */
  @Override
  public abstract Prel getPlan();

  /**
   * Acquires the rexnode expression for DMLs with Update (i.e update only, Merge with updates)
   *
   * <p>if update column index is present (i.e., the value updated), use the update expression
   * value. <br>
   * else (i.e., the value is updated) use the original value. leftFieldCount is only present for
   * Copy On Write Join Plan. Thus, if Merge on Read plan, keep track of parsed
   * OutdatedTargetColumns to ensure indexing is in sync with input-node references
   *
   * <p>The update rexNode's index varies per Dml Write Mode (Merge on Read vs Copy on Write).
   */
  protected RexNode getUpdateExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      Integer insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition) {
    Integer updateColumnIndex = updateColumnsWithIndex.get(field.getName());

    // the value is not updated, use the original column value
    if (updateColumnIndex == null) {
      int index =
          leftFieldCount.isPresent()
              ? field.getIndex()
              : field.getIndex() - parsedOutdatedTargetColumns;
      return rexBuilder.makeInputRef(field.getType(), index);
    }

    return getUpdateRexNodeExpr(
        rexBuilder,
        field,
        updateColumnIndex,
        leftFieldCount,
        insertedFieldCount,
        rowIndexNullCondition,
        rightRowIndexNullCondition);
  }

  /**
   * get the 'update' Expression for update_insert merge.
   *
   * @param rexBuilder Calcite's RexNode Builder
   * @param field the Rex field at reference
   * @param leftFieldCount starting index of input
   * @param insertedFieldCount started index of insert cols
   * @param rowIndexNullCondition null condition Case statement. Used to select correct input ref
   * @param rightRowIndexNullCondition null condition case statement for input Sys cols on right
   *     side of join.
   * @return Update Expression Node, which is one of the two inputs to the InsertExpr case
   *     statement.
   */
  abstract RexNode getUpdateInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      Integer insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition);

  /**
   * Acquires the update column index for the DML Plan.
   *
   * @param rexBuilder rexnode builder
   * @param field the table column/field
   * @param updateColumnIndex field index of the update column
   * @param leftFieldCount total fields on the left side of join. (Copy on Write only)
   * @param insertedFieldCount starting index of the insert fields. (Copy on Write only)
   * @param rightRowIndexNullCondition (Copy on Write only) RexNode IS NULL Condition on System
   *     column.
   * @return Expression which calculates index of the correct update output col
   */
  abstract RexNode getUpdateRexNodeExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      int updateColumnIndex,
      Optional<Integer> leftFieldCount,
      int insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition);

  /** Utility function to apply IS_NULL(col) filter for the given input node */
  protected Prel addColumnIsNullFilter(RelNode inputNode, RelDataType fieldType, int fieldIndex) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexNode filterCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef(fieldType, fieldIndex));

    return FilterPrel.create(
        inputNode.getCluster(), inputNode.getTraitSet(), inputNode, filterCondition);
  }

  /**
   * Extract "When Not Matched" rows from 'input' data layout from Calcite's Inserted Data Area
   * (Exp(userCol1),...,Exp(userColN)), System Columns(filePath, rowIndex)
   */
  protected Prel getInsertOnlyMergeInputDataPlan(int filePathSysColIndex, RelNode inputRel) {

    // filter out matched rows since we are doing "When Not Matched"
    RelDataTypeField filePathField =
        inputRel.getRowType().getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);

    return addColumnIsNullFilter(inputRel, filePathField.getType(), filePathSysColIndex);
  }

  /**
   * InsertOnlyMergePlan is essentially a wrapper on insertOnlyMergeInputDataPlan. The inserted data
   * columns names from Calcite is in expression format (e.g., $0) We need map the names to user
   * column names so that the downstream plan can consume.
   */
  protected Prel getInsertOnlyMergePlan(Prel insertOnlyMergeInputDataPlan) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // project (tablecol1, ..., tablecolN)
    List<RelDataTypeField> projectFields =
        insertOnlyMergeInputDataPlan.getRowType().getFieldList().stream()
            .filter(f -> !isSystemColumn(f.getName()))
            .collect(Collectors.toList());

    List<String> projectNames = getInsertOnlyProjectNames(projectFields);

    List<RexNode> projectExprs = new ArrayList<>();
    for (RelDataTypeField field : projectFields) {
      RexNode projectExpr = rexBuilder.makeInputRef(field.getType(), field.getIndex());
      projectExprs.add(projectExpr);
    }

    RelDataType projectRowType =
        RexUtil.createStructType(
            insertOnlyMergeInputDataPlan.getCluster().getTypeFactory(),
            projectExprs,
            projectNames,
            null);

    return ProjectPrel.create(
        insertOnlyMergeInputDataPlan.getCluster(),
        insertOnlyMergeInputDataPlan.getTraitSet(),
        insertOnlyMergeInputDataPlan,
        projectExprs,
        projectRowType);
  }

  /**
   * @return the project names designated for the output. Acquisition of project names varies per
   *     Dml Write Mode Type
   */
  abstract List<String> getInsertOnlyProjectNames(List<RelDataTypeField> projectFields);

  /**
   * Determine official nodes passed update_insert output. The WHEN MATCHED case is determined from
   * previous method ({@link #getUpdateInsertExpr}. This method is responsible for determining the
   * correct node for WHEN NOT MATCHED case.
   *
   * @param rexBuilder Calcite's RexNode Builder
   * @param field the Rex field of reference
   * @param fieldCount starting index of input
   * @param RowIndexFieldNullCheck null condition on Sys Col Case statement. Used to select correct
   *     input ref.
   * @param updateExprForInsert The node Selected for WHEN MATCHED case
   * @return the (WHEN NOT MATCHED || WHEN MATCHED) case as a package.
   */
  abstract RexNode getMergeWithInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> fieldCount,
      RexNode RowIndexFieldNullCheck,
      RexNode updateExprForInsert);

  /**
   * Project input columns based on operation type:
   *
   * <p>CASE DELETE: The input fields and iterated fields will only be system columns.
   *
   * <p>CASE: UPDATE, MERGE_UPDATE_ONLY: Project the update cols if present... See {@link
   * #getUpdateExpr}
   *
   * <p>CASE: MERGE_UPDATE_INSERT: pass output from getUpdateExpr into Merge-Specific update... see
   * {@link #getMergeWithInsertExpr}... output will Project CASE(getUpdateExpr output vs.
   * insert-column index output).
   *
   * <p>CASE: MERGE_INSERT_ONLY: Not possible to reach here. Handled elsewhere.
   */
  protected RexNode getColumnProjectExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      int index,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition) {
    switch (operation) {
      case DELETE:
        return getDeleteExpr(rexBuilder, field, index);
      case UPDATE:
        return getUpdateExpr(
            rexBuilder,
            field,
            leftFieldCount,
            0,
            rowIndexNullCondition,
            rightRowIndexNullCondition);
      case MERGE:
        switch (mergeType) {
          case UPDATE_ONLY:
            return getUpdateExpr(
                rexBuilder,
                field,
                leftFieldCount,
                0,
                rowIndexNullCondition,
                rightRowIndexNullCondition);
          case UPDATE_INSERT:
            RexNode updateExprWithInsertedColumns =
                getUpdateInsertExpr(
                    rexBuilder,
                    field,
                    leftFieldCount,
                    leftFieldCount
                        .map(integer -> integer - DmlUtils.SYSTEM_COLUMN_COUNT)
                        .orElse(-1),
                    rowIndexNullCondition,
                    rightRowIndexNullCondition);
            return getMergeWithInsertExpr(
                rexBuilder,
                field,
                leftFieldCount,
                rowIndexNullCondition,
                updateExprWithInsertedColumns);
          default:
            throw new UnsupportedOperationException(
                String.format("Unrecoverable Error: Invalid type: %s", mergeType));
        }
      default:
        throw new UnsupportedOperationException(
            String.format("Unrecoverable Error: Invalid type: %s", operation));
    }
  }

  abstract RexNode getDeleteExpr(RexBuilder rexBuilder, RelDataTypeField field, int index);

  public static DistributionTrait getHashDistributionTraitForFields(
      RelDataType rowType, List<String> columnNames) {
    ImmutableList<DistributionField> fields =
        columnNames.stream()
            .map(
                n ->
                    new DistributionTrait.DistributionField(
                        Preconditions.checkNotNull(rowType.getField(n, false, false)).getIndex()))
            .collect(ImmutableList.toImmutableList());
    return new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, fields);
  }
}
