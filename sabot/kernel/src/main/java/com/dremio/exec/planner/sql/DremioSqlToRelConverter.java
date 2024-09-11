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

import static com.dremio.exec.util.ColumnUtils.isSystemColumn;

import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.handlers.query.SupportsSelection;
import com.dremio.exec.planner.sql.handlers.query.SupportsSqlToRelConversion;
import com.dremio.exec.planner.sql.handlers.query.SupportsTransformation;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
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
import org.apache.calcite.sql.SqlSelect;
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
    LogicalTableModify logicalTableModify;
    switch (query.getKind()) {
      case DELETE:
        setSqlKindInRelContext(query.getKind());
        logicalTableModify =
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
                dmlWriteMode),
            query.getKind());
      case MERGE:
        setSqlKindInRelContext(query.getKind());
        return RelRoot.of(
            convertMerge((SqlMerge) query, getTargetTable(query), dmlWriteMode), query.getKind());
      case UPDATE:
        setSqlKindInRelContext(query.getKind());
        logicalTableModify =
            (LogicalTableModify) (super.convertQueryRecursive(query, top, targetRowType).rel);
        return RelRoot.of(
            TableModifyCrel.create(
                getTargetTable(query),
                catalogReader,
                dmlWriteMode == RowLevelOperationMode.MERGE_ON_READ
                    ? projectDatedColumnsForDmlUpdate(logicalTableModify, getTargetTable(query))
                    : logicalTableModify.getInput(),
                LogicalTableModify.Operation.UPDATE,
                logicalTableModify.getUpdateColumnList(),
                logicalTableModify.getSourceExpressionList(),
                false,
                null,
                hasSource,
                dmlWriteMode),
            query.getKind());
      case OTHER:
        return convertOther(query, top, targetRowType);
      default:
        return super.convertQueryRecursive(query, top, targetRowType);
    }
  }

  /** Set the SqlKind in the RelContext */
  private void setSqlKindInRelContext(SqlKind operation) {
    if (toRelContext instanceof DremioToRelContext.DremioQueryToRelContext) {
      ((DremioToRelContext.DremioQueryToRelContext) toRelContext).setSqlKind(operation);
    }
  }

  /**
   * Discard the outdated target columns included in the update call.
   *
   * <p>A target column is considered outdated if the column's name appears in the update list.
   * Method exclusively applies to Merge-On-Read updates. We must exclude any outdated target
   * columns from the update call to ensure we fit the expected 'input' schema during physical
   * planning
   */
  private RelNode projectDatedColumnsForDmlUpdate(
      LogicalTableModify logicalTableModify, RelOptTable targetTable) {
    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RelNode relNode = logicalTableModify.getInput();
    RelDataType rowType = relNode.getRowType();
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RexNode> projectedColumns = new ArrayList<>();

    List<String> partitionColumns =
        ((DremioPrepareTable) targetTable)
            .getTable()
            .getDatasetConfig()
            .getReadDefinition()
            .getPartitionColumnsList();
    Set<String> outdatedTargetColumns =
        DmlUtils.getOutdatedTargetColumns(
            new HashSet<>(logicalTableModify.getUpdateColumnList()), targetTable, partitionColumns);

    // Only apply the filter when indexing over the Target Columns.
    // target columns come before the system columns.
    boolean scanningTargetColumns = true;
    for (RelDataTypeField field : fields) {
      if (scanningTargetColumns && outdatedTargetColumns.contains(field.getName())) {
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
      RelNode relNode = null;
      if (sqlNode instanceof SupportsSelection) {
        relNode =
            convertQueryRecursive(
                    ((SupportsSelection) sqlNode).getSourceSelect(), top, targetRowType)
                .rel
                .getInput(0);
      } else if (sqlNode instanceof SupportsTransformation) {
        SqlSelect transformationsSelect =
            ((SupportsTransformation) sqlNode).getTransformationsSelect();
        if (transformationsSelect != null) {
          // the relNode will contain the transformation expressions
          relNode = convertQueryRecursive(transformationsSelect, top, targetRowType).rel;
        }
      }

      return RelRoot.of(
          ((SupportsSqlToRelConversion) sqlNode)
              .convertToRel(cluster, catalogReader, relNode, toRelContext),
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

  /**
   * This is a copy of Calcite's convertMerge(). The indexing of the Projection RelNode differs
   * between MOR vs. COW return Dremio's TableModifyCrel
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

    RelNode mergeSourceRel = convertSelect(call.getSourceSelect(), false);
    RelNode sourceInputRel = mergeSourceRel.getInput(0);

    // then, convert the insert statement so we can get the insert
    // values expressions
    SqlInsert insertCall = call.getInsertCall();
    List<RexNode> insertProjects = new ArrayList<>();
    if (insertCall != null) {
      insertProjects = flattenInsertProjects(insertCall);
    }

    final List<RexNode> projects = new ArrayList<>();
    final LogicalProject mergeSourceProject = (LogicalProject) mergeSourceRel;
    final int sourceRelColumnCount = mergeSourceProject.getProjects().size();
    final int extendedColumnCount = getExtendedColumnCount(targetTable);

    switch (dmlOpMode) {
      case MERGE_ON_READ:
        List<String> partitionColumns =
            ((DremioPrepareTable) targetTable)
                .getTable()
                .getDatasetConfig()
                .getReadDefinition()
                .getPartitionColumnsList();
        Set<String> outdatedTargetColumns =
            DmlUtils.getOutdatedTargetColumns(
                new HashSet<>(updateColumnNameList), targetTable, partitionColumns);

        mergeOnReadConvertMerge(
            sourceRelColumnCount,
            insertProjects,
            updateCall,
            insertCall,
            mergeSourceProject,
            projects,
            targetRowType,
            outdatedTargetColumns);
        break;
      case COPY_ON_WRITE:
        copyOnWriteConvertMerge(
            sourceRelColumnCount,
            insertProjects,
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
        dmlOpMode);
  }

  private List<RexNode> flattenInsertProjects(SqlInsert insertCall) {
    RelNode insertRel = convertInsert(insertCall);

    List<RexNode> level1InsertExprs = null;
    List<RexNode> level2InsertExprs = null;
    List<RexNode> insertProjects = new ArrayList<>();
    // if there are 2 level of projections in the insert source, combine
    // them into a single project; level1 refers to the topmost project;
    // the level1 projection contains references to the level2
    // expressions, except in the case where no target expression was
    // provided, in which case, the expression is the default value for
    // the column; or if the expressions directly map to the source
    // table
    level1InsertExprs = ((LogicalProject) insertRel.getInput(0)).getProjects();
    if (insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
      level2InsertExprs = ((LogicalProject) insertRel.getInput(0).getInput(0)).getProjects();
    }
    // Only include user columns (no extend columns)
    int nLevel1Exprs = level1InsertExprs.size();

    for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
      if ((level2InsertExprs != null)
          && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
        int level2Idx = ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
        insertProjects.add(level2InsertExprs.get(level2Idx));
      } else {
        insertProjects.add(level1InsertExprs.get(level1Idx));
      }
    }
    return insertProjects;
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
      int totalInputRelColumns,
      List<RexNode> insertProjects,
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
      int start = totalInputRelColumns - updateColCount - targetColCount;

      // add Target Col & System Columns. exclude any outdated target columns referenced.
      for (int i = start; i < totalInputRelColumns - updateColCount; i++) {
        if (outdatedTargetColumnIndex.contains(i - start)) {
          continue;
        }
        projects.add(project.getProjects().get(i));
      }

      // Add InsertCols. This should be ok as long as Inserted Source Cols map to "sourceProjects"
      projects.addAll(insertProjects);

      // Add UpdateCols. This should be good. total - updateColumns
      projects.addAll(Util.skip(project.getProjects(), totalInputRelColumns - updateColCount));
    } else if (insertCall != null) {
      // Add Target original cols, System Cols, then Source Insert Cols... (in this order)

      // Start = target cols = total - targetRowType Cols (target cols + system cols)
      final int start = totalInputRelColumns - targetColCount;

      // Add TargetCol + extended
      for (int i = start; i < totalInputRelColumns; i++) {
        if (outdatedTargetColumnIndex.contains(i - start)) {
          continue;
        }
        projects.add(project.getProjects().get(i));
      }

      // Add insertCols
      projects.addAll(insertProjects);
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

  /**
   * 1. columns projected from * join: inserted columns + system columns(i.e., filetPath, rowIndex)
   * + updated columns
   */
  private void copyOnWriteConvertMerge(
      int sourceRelColumnCount,
      List<RexNode> insertProjects,
      SqlUpdate updateCall,
      SqlInsert insertCall,
      LogicalProject project,
      List<RexNode> projects,
      int extendedColumnCount) {
    if (updateCall != null) {
      // only keep extended columns (i.e., filePath, rowIndex) and updated columns
      projects.addAll(insertProjects);
      projects.addAll(
          Util.skip(
              project.getProjects(),
              sourceRelColumnCount
                  - extendedColumnCount
                  - updateCall.getTargetColumnList().size()));

    } else if (insertCall != null) {
      // for insert only merge, keep extended columns (i.e., filePath, rowIndex) for downstream to
      // filter out matched rows
      projects.addAll(insertProjects);
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
