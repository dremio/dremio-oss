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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.sql.parser.DmlUtils.SYSTEM_COLUMN_COUNT;
import static com.dremio.exec.util.ColumnUtils.isSystemColumn;

import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.handlers.query.SupportsSelection;
import com.dremio.exec.planner.sql.handlers.query.SupportsSqlToRelConversion;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.RowLevelOperationMode;

/** An overridden implementation of SqlToRelConverter that redefines view expansion behavior. */
public class DremioSqlToRelConverter extends SqlToRelConverter {

  private final ToRelContext toRelContext;

  public DremioSqlToRelConverter(
      ToRelContext toRelContext,
      RelOptCluster relOptCluster,
      DremioCatalogReader dremioCatalogReader,
      SqlValidator validator,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(toRelContext, validator, dremioCatalogReader, relOptCluster, convertletTable, config);
    this.toRelContext = toRelContext;
  }

  @Override
  public RelNode toRel(RelOptTable table, @Nonnull List<RelHint> hints) {
    final RelNode rel = table.toRel(toRelContext);
    final RelNode scan =
        rel instanceof Hintable && CollectionUtils.isNotEmpty(hints)
            ? SqlUtil.attachRelHint(hintStrategies, hints, (Hintable) rel)
            : rel;
    return scan;
  }

  /**
   * Further compacting LiteralValues into a single LogicalValues When converting values, Calcite
   * would: // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of // literals into a
   * single LogicalValues; this gives the optimizer a smaller // input tree. For everything else
   * (computed expressions, row // sub-queries), we union each row in as a projection on top of a //
   * LogicalOneRow. Calcite decides it is a literal value iif SqlLiteral. However, there are some
   * SqlNodes which could be resolved to literals. For example, CAST('71543.41' AS DOUBLE) in
   * DX-34244 During Calcite's convertValues(), some of those nodes would be resolved into
   * RexLiteral. This override function will check the result of Calcite's convertValues(), and do
   * further compaction based on resolved RexLiteral.
   */
  @Override
  public RelNode convertValues(SqlCall values, RelDataType targetRowType) {

    RelNode ret = super.convertValues(values, targetRowType);
    if (ret instanceof LogicalValues) {
      return ret;
    }

    // ret could be LogicalProject if there is only one row
    if (ret instanceof LogicalUnion) {
      LogicalUnion union = (LogicalUnion) ret;
      ImmutableList.Builder<ImmutableList<RexLiteral>> literalRows = ImmutableList.builder();
      for (RelNode input : union.getInputs()) {
        if (!(input instanceof LogicalProject)) {
          return ret;
        }
        LogicalProject project = (LogicalProject) input;
        ImmutableList.Builder<RexLiteral> literalRow = ImmutableList.builder();
        for (RexNode rexValue : project.getProjects()) {
          if (!(rexValue instanceof RexLiteral)) {
            // Return Calcite's results once saw a non-RexLiteral.
            // Consider to do further optimization of return a new Union combining rows with
            // non-RelLiteral and LogicalValues for consecutive block of RelLiteral rows
            return ret;
          } else {
            literalRow.add((RexLiteral) rexValue);
          }
        }
        literalRows.add(literalRow.build());
      }

      final RelDataType rowType;
      if (targetRowType != null) {
        rowType = targetRowType;
      } else {
        rowType =
            SqlTypeUtil.promoteToRowType(typeFactory, validator.getValidatedNodeType(values), null);
      }

      return LogicalValues.create(cluster, rowType, literalRows.build());
    }
    return ret;
  }

  @Override
  public RelNode flattenTypes(RelNode rootRel, boolean restructure) {
    RelStructuredTypeFlattener typeFlattener =
        new RelStructuredTypeFlattener(rexBuilder, toRelContext, restructure);
    return typeFlattener.rewrite(rootRel);
  }

  /**
   * TODO: need to find a better way to fix validations for Optimize command.
   *
   * @param query Query to convert
   * @param top Whether the query is top-level, say if its result will become a JDBC result set;
   *     <code>false</code> if the query will be part of a view.
   * @return
   */
  @Override
  protected RelRoot convertQueryRecursive(SqlNode query, boolean top, RelDataType targetRowType) {
    // Cast query to SqlDmlOperator if it matches the type
    SqlDmlOperator sqlDmlOperator = query instanceof SqlDmlOperator ? (SqlDmlOperator) query : null;

    boolean hasSource = sqlDmlOperator != null && sqlDmlOperator.getSourceTableRef() != null;

    // Assign dmlWriteMode based on the presence of sqlDmlOperator
    RowLevelOperationMode dmlWriteMode =
        sqlDmlOperator != null ? sqlDmlOperator.getDmlWriteMode() : null;

    switch (query.getKind()) {
      case DELETE:
        LogicalTableModify logicalTableModify =
            (LogicalTableModify) (super.convertQueryRecursive(query, top, targetRowType).rel);
        Preconditions.checkNotNull(logicalTableModify);
        return RelRoot.of(
            TableModifyCrel.create(
                getTargetTable(query),
                catalogReader,
                logicalTableModify.getInput(),
                LogicalTableModify.Operation.DELETE,
                null,
                null,
                false,
                null,
                hasSource,
                null,
                dmlWriteMode),
            query.getKind());
      case MERGE:
        return RelRoot.of(
            convertMerge((SqlMerge) query, getTargetTable(query), dmlWriteMode), query.getKind());
      case UPDATE:
        logicalTableModify =
            (LogicalTableModify) (super.convertQueryRecursive(query, top, targetRowType).rel);
        return RelRoot.of(
            TableModifyCrel.create(
                getTargetTable(query),
                catalogReader,
                dmlWriteMode == RowLevelOperationMode.MERGE_ON_READ
                    ? projectDatedColumnsForDmlUpdate(logicalTableModify)
                    : logicalTableModify.getInput(),
                LogicalTableModify.Operation.UPDATE,
                logicalTableModify.getUpdateColumnList(),
                logicalTableModify.getSourceExpressionList(),
                false,
                null,
                hasSource,
                null,
                dmlWriteMode),
            query.getKind());
      case OTHER:
        return convertOther(query, top, targetRowType);
      default:
        return super.convertQueryRecursive(query, top, targetRowType);
    }
  }

  /**
   * discard the target columns included in the update call. A target column is considered outdated
   * if the column's name appears in the update list. Method exclusively applies to Merge-On-Read
   * updates.
   */
  private RelNode projectDatedColumnsForDmlUpdate(LogicalTableModify logicalTableModify) {
    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RelNode relNode = logicalTableModify.getInput();
    RelDataType rowType = relNode.getRowType();
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RexNode> projectedColumns = new ArrayList<>();

    // Only apply the filter when indexing over the Target Columns.
    // target columns come before the system columns.
    boolean scanningTargetColumns = true;
    for (RelDataTypeField field : fields) {
      if (scanningTargetColumns
          && logicalTableModify.getUpdateColumnList().contains(field.getName())) {
        continue;
      } else {
        RexNode rexNode = rexBuilder.makeInputRef(relNode, field.getIndex());
        projectedColumns.add(rexNode);
      }

      // target cols are passed once system cols exist
      if (isSystemColumn(field.getName())) {
        scanningTargetColumns = false;
      }
    }
    return relBuilder.push(relNode).project(projectedColumns).build();
  }

  /**
   * RelNode for OTHER sql kind
   *
   * @param sqlNode
   * @return
   */
  private RelRoot convertOther(SqlNode sqlNode, boolean top, RelDataType targetRowType) {
    if (sqlNode instanceof SupportsSqlToRelConversion) {
      RelNode inputRel = null;
      if (sqlNode instanceof SupportsSelection) {
        inputRel =
            convertQueryRecursive(
                    ((SupportsSelection) sqlNode).getSourceSelect(), top, targetRowType)
                .rel
                .getInput(0);
      }

      return RelRoot.of(
          ((SupportsSqlToRelConversion) sqlNode)
              .convertToRel(cluster, catalogReader, inputRel, toRelContext),
          SqlKind.OTHER);
    }

    return super.convertQueryRecursive(sqlNode, top, targetRowType);
  }

  private static int getExtendedColumnCount(RelOptTable table) {
    if (!(table instanceof DremioPrepareTable)) {
      return 0;
    }
    DremioPrepareTable dremioPrepareTable = (DremioPrepareTable) table;
    DremioTable dremioTable = dremioPrepareTable.getTable();
    if (!(dremioTable instanceof ExtensibleTable)) {
      return 0;
    }
    return dremioTable.getSchema().getFieldCount()
        - ((ExtensibleTable) dremioTable).getExtendedColumnOffset();
  }

  public static class ConsecutiveProjectsCounterForJoin extends StatelessRelShuttleImpl {
    private Integer consecutiveProjectsCount = null;

    public static int getCount(RelNode root) {
      ConsecutiveProjectsCounterForJoin counter = new ConsecutiveProjectsCounterForJoin();
      root.accept(counter);
      return counter.consecutiveProjectsCount == null ? 0 : counter.consecutiveProjectsCount;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
      // only check the first join
      if (consecutiveProjectsCount == null) {
        consecutiveProjectsCount = getConsecutiveProjectsCountFromRoot(join.getInput(0));
      }
      return join;
    }
  }

  private static int getConsecutiveProjectsCountFromRoot(RelNode root) {
    Preconditions.checkNotNull(root);
    int projectCount = 0;
    RelNode node = root;
    while (node instanceof LogicalProject) {
      projectCount++;
      if (node.getInputs().size() != 1) {
        break;
      }
      node = node.getInput(0);
    }

    return projectCount;
  }

  /***
   * the left side of the join in rewritten merge is the source
   * the insertRel converted from insertCall is based on the source, plus one or two extra projects, depends on Insert clause.
   * to determine how many extra projected are added, we use: consecutive Projects from converted insert node substracts the consecutive Projects from the source node
   */
  private int getProjectLevelsOnTopOfInsertSource(RelNode insertRel, RelNode mergeSourceRel) {
    int consecutiveProjectsCountFromSourceNode =
        ConsecutiveProjectsCounterForJoin.getCount(mergeSourceRel);
    int consecutiveProjectsCountFromInsertNode = getConsecutiveProjectsCountFromRoot(insertRel);
    return consecutiveProjectsCountFromInsertNode - consecutiveProjectsCountFromSourceNode;
  }

  /**
   * This is a copy of Calcite's convertMerge(), with some modifications: 1. columns projected from
   * join: inserted columns + system columns(i.e., filetPath, rowIndex) + updated columns 2. return
   * Dremio's TableModifyCrel
   */
  private RelNode convertMerge(
      SqlMerge call, RelOptTable targetTable, RowLevelOperationMode dmlOpMode) {
    // convert update column list from SqlIdentifier to String
    final List<String> updateColumnNameList = new ArrayList<>();
    final RelDataType targetRowType = targetTable.getRowType();
    SqlUpdate updateCall = call.getUpdateCall();
    if (updateCall != null) {
      for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
        SqlIdentifier id = (SqlIdentifier) targetColumn;
        RelDataTypeField field =
            SqlValidatorUtil.getTargetField(
                targetRowType, typeFactory, id, catalogReader, targetTable);
        assert field != null : "column " + id.toString() + " not found";
        updateColumnNameList.add(field.getName());
      }
    }

    // replace the projection of the source select with a
    // projection that contains the following:
    // 1) the expressions corresponding to the new insert row (if there is
    //    an insert)
    // 2) all columns from the target table (if there is an update)
    // 3) the set expressions in the update call (if there is an update)

    // first, convert the merge's source select to construct the columns
    // from the target table and the set expressions in the update call
    RelNode mergeSourceRel = convertSelect(call.getSourceSelect(), false);
    RelNode sourceInputRel = mergeSourceRel.getInput(0);

    // then, convert the insert statement so we can get the insert
    // values expressions
    SqlInsert insertCall = call.getInsertCall();
    int nLevel1Exprs = 0;
    List<RexNode> level1InsertExprs = null;
    List<RexNode> level2InsertExprs = null;
    List<RexNode> sourceProjects = new ArrayList<>();
    final LogicalProject mergeSourceProject = (LogicalProject) mergeSourceRel;

    Map<String, Integer> outdatedTargetColumns = new HashMap<>();

    for (String updateCol : updateColumnNameList) {
      outdatedTargetColumns.put(updateCol, 1);
    }

    if (insertCall != null) {

      // recall:
      // - For MERGE_insert_only dml, a target column is considered outdated if the column
      //   name appeared in the insert call. Update call DNE in this case. Total tally = 1.
      // - For MERGE_update_insert dml, a target column is considered outdated if
      //   the name appears in the BOTH insert call AND the update call. Total tally = 2.
      int requiredTallyForInsertOnly = 1;
      int requiredTallyForUpdateInsert = 2;

      int requiredCountToBeOutdated =
          (updateCall == null) ? requiredTallyForInsertOnly : requiredTallyForUpdateInsert;

      findOutdatedTargetColumns(
          insertCall, outdatedTargetColumns, targetRowType, targetTable, requiredCountToBeOutdated);

      RelNode insertRel = convertInsert(insertCall);
      RelNode insertInput = insertRel.getInput(0);
      int insertColumnCount = insertInput.getRowType().getFieldCount();
      int insertAddedProjects = getProjectLevelsOnTopOfInsertSource(insertInput, sourceInputRel);

      // there are cases where there are no added project by Insert call
      // 1. the source table sub-query is already topped with a Project
      // (e.g., select less columns than referenced columns in the source sub-query).
      // 2. the insert call only reference columns output from 1 (e.g., no literal values are
      // inserted)
      // In those cases, we add the projected columns from merge source directly
      if (insertAddedProjects == 0) {
        sourceProjects = mergeSourceProject.getProjects().subList(0, insertColumnCount);
      } else {
        // if there are 2 level of projections in the insert source, combine
        // them into a single project; level1 refers to the topmost project;
        // the level1 projection contains references to the level2
        // expressions, except in the case where no target expression was
        // provided, in which case, the expression is the default value for
        // the column; or if the expressions directly map to the source
        // table
        level1InsertExprs = ((LogicalProject) insertRel.getInput(0)).getProjects();

        if (insertAddedProjects > 1
            && insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
          level2InsertExprs = ((LogicalProject) insertRel.getInput(0).getInput(0)).getProjects();
        }
        // Only include user columns (no extend columns)
        nLevel1Exprs = level1InsertExprs.size();

        for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
          if ((level2InsertExprs != null)
              && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
            int level2Idx = ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
            sourceProjects.add(level2InsertExprs.get(level2Idx));
          } else {
            sourceProjects.add(level1InsertExprs.get(level1Idx));
          }
        }
      }
    }

    final List<RexNode> projects = new ArrayList<>();
    final int sourceRelColumnCount = mergeSourceProject.getProjects().size();
    final int extendedColumnCount = getExtendedColumnCount(targetTable);

    switch (dmlOpMode) {
      case MERGE_ON_READ:
        mergeOnReadConvertMerge(
            sourceRelColumnCount,
            sourceProjects,
            updateCall,
            insertCall,
            mergeSourceProject,
            projects,
            targetRowType,
            outdatedTargetColumns.keySet());
        break;
      case COPY_ON_WRITE:
        copyOnWriteConvertMerge(
            sourceRelColumnCount,
            sourceProjects,
            updateCall,
            insertCall,
            mergeSourceProject,
            projects,
            extendedColumnCount);
        break;
    }

    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);

    relBuilder.push(sourceInputRel).project(projects);

    return TableModifyCrel.create(
        targetTable,
        catalogReader,
        relBuilder.build(),
        LogicalTableModify.Operation.MERGE,
        null,
        null,
        false,
        updateColumnNameList,
        true,
        outdatedTargetColumns.keySet(),
        dmlOpMode);
  }

  /**
   * Helper method used to locate outdated target columns. It is only possible to hit this if an
   * Insert Command is included in the SQL Merge Operation.
   *
   * <p>A Target Column is considered outdated under one of the following Conditions: <br>
   * - insert-update Merge op where the referenced column(s) name exists in both the update AND
   * insert call <br>
   * - insert-only Merge op where the referenced column(s) name exists in the insert call <br>
   * - update-only Merge op where the referenced column(s) name exist in the update call <br>
   *
   * <p>If the following conditions are met, then they are added to the OutdatedTargetColumn list
   *
   * @param insertCall the insert call attached to the merge call
   * @param columnNameCount key: the map of column names found in the insert and update call. Value:
   *     Occurrences shared between the insert and update calls. Tally of 2 means it was found in
   *     both the update and insert call. Max is two.
   * @param requiredCountToBeOutdated The expected tally count required for a target column to be
   *     outdated, e.i. discarded from the credible "outdated target column name" list.
   */
  private void findOutdatedTargetColumns(
      SqlInsert insertCall,
      Map<String, Integer> columnNameCount,
      RelDataType targetRowType,
      RelOptTable targetTable,
      int requiredCountToBeOutdated) {

    // if insertCall is nonNull while insert column list is null, then the user performed 'insert *'
    if (insertCall.getTargetColumnList() == null) {
      if (columnNameCount.isEmpty()) {
        for (int i = 0; i < targetRowType.getFieldCount() - SYSTEM_COLUMN_COUNT; i++) {
          columnNameCount.put(targetRowType.getFieldList().get(i).getName(), 1);
        }
      } else {
        columnNameCount.replaceAll((k, v) -> columnNameCount.get(k) + 1);
      }
    } else {
      for (SqlNode insertTargetColumn : insertCall.getTargetColumnList()) {
        SqlIdentifier id = (SqlIdentifier) insertTargetColumn;
        RelDataTypeField field =
            SqlValidatorUtil.getTargetField(
                targetRowType, typeFactory, id, catalogReader, targetTable);
        assert field != null : "column " + id.toString() + " not found";
        columnNameCount.put(field.getName(), columnNameCount.getOrDefault(field.getName(), 0) + 1);
      }
    }

    // keep columns that only exist in both the update call and insert call
    columnNameCount.entrySet().removeIf(entry -> entry.getValue() != requiredCountToBeOutdated);
  }

  /**
   * Acquire the input cols for DML Physical plan in the correct order. recall: the cols in project
   * are provided in the following order: <br>
   * 1) Source Insert Cols (if exist) <br>
   * 2.A) Target Original Cols ----> targetRowType fields are composed of original + system cols
   * <br>
   * 2.B) Target System Cols ----/ <br>
   * 3) Update Cols (if exist) <br>
   * <br>
   * We want to adjust this order to be the following for Merge-on-Read Physical Plan: <br>
   * 1.A) Target Original Cols (outdated cols removed) <br>
   * 1.B) Target System Cols <br>
   * 2) Source Insert Cols <br>
   * 3) Update Cols <br>
   */
  private void mergeOnReadConvertMerge(
      int sourceRelColumnCount,
      List<RexNode> sourceProjects,
      SqlUpdate updateCall,
      SqlInsert insertCall,
      LogicalProject project,
      List<RexNode> projects,
      RelDataType targetRowType,
      Set<String> outdatedTargetColumns) {
    int targetColCount = targetRowType.getFieldCount();

    Set<Integer> outdatedTargetColumnIndex =
        buildOutdatedColumnsIndexList(outdatedTargetColumns, targetRowType);

    if (updateCall != null) {
      int updateColCount = updateCall.getTargetColumnList().size();

      // Start = target original cols = total - updateCount - target total (target cols + system
      // cols)...
      // This should be ok... because source insert cols always show up first in project
      int start = sourceRelColumnCount - updateColCount - targetColCount;

      // add Target Col & System Columns. exclude any outdated target columns referenced.
      for (int i = start; i < sourceRelColumnCount - updateColCount; i++) {
        if (outdatedTargetColumnIndex.contains(i - start)) {
          continue;
        }
        projects.add(project.getProjects().get(i));
      }

      // Add InsertCols. This should be ok as long as Inserted Source Cols map to "sourceProjects"
      projects.addAll(sourceProjects);

      // Add UpdateCols. This should be good. total - updateColumns
      projects.addAll(Util.skip(project.getProjects(), sourceRelColumnCount - updateColCount));
    } else if (insertCall != null) {
      // Add Target original cols, System Cols, then Source Insert Cols... (in this order)

      // Start = target cols = total - targetRowType Cols (target cols + system cols)
      final int start = sourceRelColumnCount - targetColCount;

      // Add TargetCol + extended
      for (int i = start; i < sourceRelColumnCount; i++) {
        if (outdatedTargetColumnIndex.contains(i - start)) {
          continue;
        }
        projects.add(project.getProjects().get(i));
      }

      // Add insertCols
      projects.addAll(sourceProjects);
    }
  }

  /**
   * Acquire a list of index references to the outdated column names. This list is soon used to
   * discard outdated target columns.
   *
   * @param outdatedColumnNames
   * @param targetRowType
   * @return
   */
  private Set<Integer> buildOutdatedColumnsIndexList(
      Set<String> outdatedColumnNames, RelDataType targetRowType) {
    Set<Integer> outdatedColumnIndex = new HashSet<>();

    for (int i = 0; i < targetRowType.getFieldCount(); i++) {
      if (outdatedColumnNames.contains(targetRowType.getFieldList().get(i).getName())) {
        outdatedColumnIndex.add(i);
      }
    }
    return outdatedColumnIndex;
  }

  private void copyOnWriteConvertMerge(
      int sourceRelColumnCount,
      List<RexNode> sourceProjects,
      SqlUpdate updateCall,
      SqlInsert insertCall,
      LogicalProject project,
      List<RexNode> projects,
      int extendedColumnCount) {
    if (updateCall != null) {
      // only keep extended columns (i.e., filePath, rowIndex) and updated columns
      projects.addAll(sourceProjects);
      projects.addAll(
          Util.skip(
              project.getProjects(),
              sourceRelColumnCount
                  - extendedColumnCount
                  - updateCall.getTargetColumnList().size()));

    } else if (insertCall != null) {
      // for insert only merge, keep extended columns (i.e., filePath, rowIndex) for downstream to
      // filter out matched rows
      projects.addAll(sourceProjects);
      projects.addAll(Util.skip(project.getProjects(), sourceRelColumnCount - extendedColumnCount));
    }
  }

  @Override
  protected RelNode convertSetOp(SqlCall call) {
    RelNode result = super.convertSetOp(call);
    RelNode left = result.getInput(0);
    RelNode right = result.getInput(1);
    List<RelDataType> types = ImmutableList.of(left.getRowType(), right.getRowType());
    RelDataType consistentType;
    if (ConsistentTypeUtil.allExactNumeric(types) && ConsistentTypeUtil.anyDecimal(types)) {
      consistentType = ConsistentTypeUtil.consistentDecimalType(typeFactory, types);
    } else {
      consistentType =
          ConsistentTypeUtil.consistentType(
              typeFactory, SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE, types);
    }
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : ImmutableList.of(left, right)) {
      if (input != consistentType) {
        convertedInputs.add(MoreRelOptUtil.createCastRel(input, consistentType));
      } else {
        convertedInputs.add(input);
      }
    }
    return result.copy(result.getTraitSet(), convertedInputs);
  }
}
