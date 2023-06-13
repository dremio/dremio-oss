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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.prepare.Prepare;
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

import com.dremio.exec.calcite.logical.CopyIntoTableCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.calcite.logical.TableOptimizeCrel;
import com.dremio.exec.calcite.logical.VacuumTableCrel;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.ops.ViewExpansionContext.ViewExpansionToken;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlVacuumTable;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * An overridden implementation of SqlToRelConverter that redefines view expansion behavior.
 */
public class DremioSqlToRelConverter extends SqlToRelConverter {

  private final SqlConverter sqlConverter;

  public DremioSqlToRelConverter(
      SqlConverter sqlConverter,
      DremioCatalogReader dremioCatalogReader,
      SqlValidator validator,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(DremioToRelContext.createQueryContext(sqlConverter), validator,
        dremioCatalogReader, sqlConverter.getCluster(), convertletTable, config);
    this.sqlConverter = sqlConverter;
  }

  @Override
  public RelNode toRel(RelOptTable table, @Nonnull List<RelHint> hints) {
    final RelNode rel = table.toRel(createToRelContext());
    final RelNode scan = rel instanceof Hintable && CollectionUtils.isNotEmpty(hints)
      ? SqlUtil.attachRelHint(hintStrategies, hints, (Hintable) rel)
      : rel;
    return scan;
  }

  public ToRelContext createToRelContext() {
    return DremioToRelContext.createQueryContext(sqlConverter);
  }

  /**
   * Further compacting LiteralValues into a single LogicalValues
   * When converting values, Calcite would:
   *     // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of
   *     // literals into a single LogicalValues; this gives the optimizer a smaller
   *     // input tree.  For everything else (computed expressions, row
   *     // sub-queries), we union each row in as a projection on top of a
   *     // LogicalOneRow.
   *  Calcite decides it is a literal value iif SqlLiteral. However, there are some SqlNodes which could be resolved to literals.
   *  For example, CAST('71543.41' AS DOUBLE) in DX-34244
   *  During Calcite's convertValues(), some of those nodes would be resolved into RexLiteral.
   *  This override function will check the result of Calcite's convertValues(), and do further compaction based on resolved RexLiteral.
   */
  @Override
  public RelNode convertValues(
    SqlCall values,
    RelDataType targetRowType) {

    RelNode ret = super.convertValues(values, targetRowType);
    if (ret instanceof LogicalValues) {
      return ret;
    }

    // ret could be LogicalProject if there is only one row
    if (ret instanceof LogicalUnion) {
      LogicalUnion union = (LogicalUnion) ret;
      ImmutableList.Builder<ImmutableList<RexLiteral>> literalRows = ImmutableList.builder();
      for (RelNode input : union.getInputs()) {
        if(!(input instanceof LogicalProject)) {
          return ret;
        }
        LogicalProject project = (LogicalProject) input;
        ImmutableList.Builder<RexLiteral> literalRow = ImmutableList.builder();
        for (RexNode rexValue : project.getProjects()) {
          if (!(rexValue instanceof RexLiteral)) {
            // Return Calcite's results once saw a non-RexLiteral.
            // Consider to do further optimization of return a new Union combining rows with non-RelLiteral and LogicalValues for consecutive block of RelLiteral rows
            return ret;
          } else {
            literalRow.add((RexLiteral) rexValue);
          }
        }
        literalRows.add(literalRow.build());
      }

      final RelDataType rowType;
      if (targetRowType!=null) {
        rowType = targetRowType;
      } else {
        rowType =
          SqlTypeUtil.promoteToRowType(
            typeFactory,
            validator.getValidatedNodeType(values),
            null);
      }

      return LogicalValues.create(cluster, rowType, literalRows.build());
    }
    return ret;
  }

  @Override
  public RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure) {
    RelStructuredTypeFlattener typeFlattener =
        new RelStructuredTypeFlattener(rexBuilder, createToRelContext(), restructure);
    return typeFlattener.rewrite(rootRel);
  }

  /**
   * TODO: need to find a better way to fix validations for Optimize command.
   * @param query           Query to convert
   * @param top             Whether the query is top-level, say if its result
   *                        will become a JDBC result set; <code>false</code> if
   *                        the query will be part of a view.
   * @return
   */

  @Override
  protected RelRoot convertQueryRecursive(SqlNode query, boolean top, RelDataType targetRowType) {
    boolean hasSource = query instanceof SqlDmlOperator && ((SqlDmlOperator)query).getSourceTableRef() != null;
    switch (query.getKind()) {
      case DELETE:
        LogicalTableModify logicalTableModify = (LogicalTableModify)(super.convertQueryRecursive(query, top, targetRowType).rel);
        Preconditions.checkNotNull(logicalTableModify);
        return RelRoot.of(TableModifyCrel.create(getTargetTable(query),  catalogReader, logicalTableModify.getInput(),
          LogicalTableModify.Operation.DELETE, null,
          null, false, null, hasSource), query.getKind());
      case MERGE:
        return RelRoot.of(convertMerge((SqlMerge)query, getTargetTable(query)), query.getKind());
      case UPDATE:
        logicalTableModify = (LogicalTableModify)(super.convertQueryRecursive(query, top, targetRowType).rel);
        return RelRoot.of(TableModifyCrel.create(getTargetTable(query),  catalogReader, logicalTableModify.getInput(),
          LogicalTableModify.Operation.UPDATE, logicalTableModify.getUpdateColumnList(),
          logicalTableModify.getSourceExpressionList(), false, null, hasSource), query.getKind());
      case OTHER:
        return convertOther(query, top, targetRowType);
      default:
        return super.convertQueryRecursive(query, top, targetRowType);
    }
  }

  /**
   * RelNode for OTHER sql kind
   * @param query
   * @return
   */
  private RelRoot convertOther(SqlNode query, boolean top, RelDataType targetRowType) {
    if (query instanceof SqlOptimize) {
      SqlOptimize optimizeNode = ((SqlOptimize)query);
      NamespaceKey path = optimizeNode.getPath();
      Prepare.PreparingTable nsTable = catalogReader.getTable(path.getPathComponents());
      //Get rel node with filters.
      RelNode rel = convertQueryRecursive(((SqlOptimize) query).getSourceSelect(), top, targetRowType).rel;
      return RelRoot.of(new TableOptimizeCrel(cluster, cluster.traitSetOf(Convention.NONE),rel.getInput(0), nsTable, null, OptimizeOptions.createInstance(optimizeNode)),
        SqlKind.OTHER);
    } else if (query instanceof SqlCopyIntoTable) {
      NamespaceKey path = ((SqlCopyIntoTable)query).getPath();
      Prepare.PreparingTable nsTable = catalogReader.getTable(path.getPathComponents());
      CopyIntoTableContext  copyIntoTableContext= new CopyIntoTableContext((SqlCopyIntoTable)query);
      return RelRoot.of(new CopyIntoTableCrel(cluster, cluster.traitSetOf(Convention.NONE), nsTable,  copyIntoTableContext), SqlKind.OTHER);
    } else if (query instanceof SqlVacuumTable) {
      NamespaceKey path = ((SqlVacuumTable) query).getPath();
      Prepare.PreparingTable nsTable = catalogReader.getTable(path.getPathComponents());
      return RelRoot.of(
        new VacuumTableCrel(cluster, cluster.traitSetOf(Convention.NONE), nsTable.toRel(createToRelContext()), nsTable,
          null, ((SqlVacuumTable) query).getVacuumOptions()), SqlKind.OTHER);
    } else {
      return super.convertQueryRecursive(query, top, targetRowType);
    }
  }

  private static int getExtendedColumnCount(RelOptTable table) {
    if (!(table instanceof DremioPrepareTable)) {
      return 0;
    }
    DremioPrepareTable dremioPrepareTable = (DremioPrepareTable)table;
    DremioTable dremioTable = dremioPrepareTable.getTable();
    if (!(dremioTable instanceof ExtensibleTable)) {
      return 0;
    }
    return dremioTable.getSchema().getFieldCount() - ((ExtensibleTable)dremioTable).getExtendedColumnOffset();
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
    while(node instanceof LogicalProject) {
      projectCount++;
      if (node.getInputs().size() != 1)  {
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
    int consecutiveProjectsCountFromSourceNode = ConsecutiveProjectsCounterForJoin.getCount(mergeSourceRel);
    int consecutiveProjectsCountFromInsertNode = getConsecutiveProjectsCountFromRoot(insertRel);
    return consecutiveProjectsCountFromInsertNode - consecutiveProjectsCountFromSourceNode;
  }

  /**
   * This is a copy of Calcite's convertMerge(), with some modifications:
   * 1. columns projected from join: inserted columns + system columns(i.e., filetPath, rowIndex) + updated columns
   * 2. return Dremio's TableModifyCrel
   */
  private RelNode convertMerge(SqlMerge call, RelOptTable targetTable) {
    // convert update column list from SqlIdentifier to String
    final List<String> targetColumnNameList = new ArrayList<>();
    final RelDataType targetRowType = targetTable.getRowType();
    SqlUpdate updateCall = call.getUpdateCall();
    if (updateCall != null) {
      for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
        SqlIdentifier id = (SqlIdentifier) targetColumn;
        RelDataTypeField field =
          SqlValidatorUtil.getTargetField(
            targetRowType, typeFactory, id, catalogReader, targetTable);
        assert field != null : "column " + id.toString() + " not found";
        targetColumnNameList.add(field.getName());
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

    if (insertCall != null) {
      RelNode insertRel = convertInsert(insertCall);

      // if there are 2 level of projections in the insert source, combine
      // them into a single project; level1 refers to the topmost project;
      // the level1 projection contains references to the level2
      // expressions, except in the case where no target expression was
      // provided, in which case, the expression is the default value for
      // the column; or if the expressions directly map to the source
      // table
      level1InsertExprs =
        ((LogicalProject) insertRel.getInput(0)).getProjects();

      if (getProjectLevelsOnTopOfInsertSource(insertRel.getInput(0), sourceInputRel) > 1
        && insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
        level2InsertExprs =
          ((LogicalProject) insertRel.getInput(0).getInput(0))
            .getProjects();
      }
      // Only include user columns (no extend columns)
      nLevel1Exprs = level1InsertExprs.size();
    }


    final List<RexNode> projects = new ArrayList<>();
    for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
      if ((level2InsertExprs != null)
        && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
        int level2Idx =
          ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
        projects.add(level2InsertExprs.get(level2Idx));
      } else {
        projects.add(level1InsertExprs.get(level1Idx));
      }
    }

    final LogicalProject project = (LogicalProject) mergeSourceRel;
    final int sourceRelColumnCount = project.getProjects().size();
    final int extendedColumnCount = getExtendedColumnCount(targetTable);
    if (updateCall != null) {
      // only keep extended columns (i.e., filePath, rowIndex) and updated columns
      // Todo:  merge-on-read version of DML implementation requires full user columns. We would revisit this approach by then
      projects.addAll(
        Util.skip(project.getProjects(),
          sourceRelColumnCount - extendedColumnCount - updateCall.getTargetColumnList().size()));
    } else if (insertCall != null){
      // for insert only merge, keep extended columns (i.e., filePath, rowIndex) for downstream to filter out matched rows
      projects.addAll(Util.skip(project.getProjects(),sourceRelColumnCount - extendedColumnCount));
    }

    RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);

    relBuilder.push(sourceInputRel)
      .project(projects);

    return TableModifyCrel.create(targetTable, catalogReader, relBuilder.build(),
      LogicalTableModify.Operation.MERGE, null,
      null, false, targetColumnNameList, true);
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
      consistentType = ConsistentTypeUtil.consistentType(typeFactory, SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE, types);
    }
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input: ImmutableList.of(left, right)) {
      if (input != consistentType) {
        convertedInputs.add(MoreRelOptUtil.createCastRel(input, consistentType));
      } else {
        convertedInputs.add(input);
      }
    }
    return result.copy(result.getTraitSet(), convertedInputs);
  }

  private static RelRoot getExpandedRelNode(NamespaceKey path,
                                            final CatalogIdentity viewOwner,
                                            final String queryString,
                                            final List<String> context,
                                            final SqlConverter sqlConverter,
                                            final BatchSchema batchSchema,
                                            final VersionContext versionContext) {
    SqlValidatorAndToRelContext.Builder builder = SqlValidatorAndToRelContext.builder(sqlConverter)
      .withSchemaPath(context)
      .withSystemDefaultParserConfig();
    if (viewOwner != null) {
      builder = builder.withUser(viewOwner);
    }
    TableVersionContext tableVersionContext = null;
    if (versionContext != null && versionContext.isSpecified()) {
      // Nested views/tables should inherit this version context (unless explicitly overridden by the nested view/table)
      builder = builder.withVersionContext(path.getRoot(), versionContext);
      tableVersionContext = TableVersionContext.of(versionContext);
    }
    SqlValidatorAndToRelContext newConverter = builder.build();
    final SqlNode parsedNode = newConverter.getSqlConverter().parse(queryString);
    final SqlNode validatedNode = newConverter.validate(parsedNode);
    if (path != null && sqlConverter.getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
      final RelRootPlus unflattenedRoot = newConverter.toConvertibleRelRoot(validatedNode, true, false, false);
      final RelRootPlus rootWithCast = adjustIcebergSchema(path, sqlConverter, unflattenedRoot);
      ExpansionNode expansionNode = (ExpansionNode) wrapExpansionNode(sqlConverter, batchSchema, path, rootWithCast, tableVersionContext);
      if (expansionNode.isDefault()) {
        sqlConverter.getViewExpansionContext().setSubstitutedWithDRR();
        sqlConverter.getFunctionContext().getContextInformation().setPlanCacheable(unflattenedRoot.isPlanCacheable());
        return rootWithCast.withExpansionNode(expansionNode);  // Successfully replaced with default raw reflection during ConvertToRel
      }
    }
    final RelRootPlus root = newConverter.toConvertibleRelRoot(validatedNode, true, true);
    sqlConverter.getFunctionContext().getContextInformation().setPlanCacheable(root.isPlanCacheable());
    if(path == null) {
      return root;
    }

    final RelRootPlus rootWithCast = adjustIcebergSchema(path, sqlConverter, root);
    return rootWithCast.withExpansionNode(ExpansionNode.wrap(path, rootWithCast.rel, rootWithCast.validatedRowType,
                                  rootWithCast.isContextSensitive(), false, tableVersionContext));
  }

  /**
   * Dremio has multiple schema types that it has to convert between:
   *
   * - Calcite schema used for planning
   * - Arrow batch schema used for query execution
   * - Iceberg schema used within Iceberg tables/views
   *
   * Calcite schema contains the most type precision and nullability whereas Arrow batch schema loses information
   * and this is propagated down into the Iceberg schemas that Dremio engine creates.  For example, Arrow does
   * not include VARCHAR precision or NOT NULL on any type.  As a result, when querying Iceberg tables and views
   * (including reflection materializations), we need to CAST the Calcite query tree underneath the table/view into
   * the table/view's lossy Arrow/Iceberg schema.
   *
   * Iceberg tables and view use {@link com.dremio.exec.store.iceberg.SchemaConverter} to convert between Iceberg schema
   * and Arrow batch schema.
   *
   * Sonar Views don't have this type mismatch problem because they store both:
   * - Arrow batch schema fields.  See {@link com.dremio.exec.util.ViewFieldsHelper#getBatchSchemaFields(BatchSchema)}
   * - Calcite fields.  See {@link com.dremio.exec.util.ViewFieldsHelper#getCalciteViewFields(DatasetConfig)}
   *
   * @param path View path
   * @param sqlConverter
   * @param root Root of View's query tree
   * @return new root with CAST if needed
   */
  private static RelRootPlus adjustIcebergSchema(NamespaceKey path, SqlConverter sqlConverter, RelRootPlus root) {
    if (!CatalogUtil.requestedPluginSupportsVersionedTables(path.getRoot(), sqlConverter.getCatalog())) {
      return root;
    }
    final JavaTypeFactoryImpl typeFactory = (JavaTypeFactoryImpl) sqlConverter.getTypeFactory();

    // Adjust the validated row type from the SqlNode
    final RelDataType validatedRowTypeNullableMaxVarchar = typeFactory.createTypeWithNullability(
      typeFactory.createTypeWithMaxVarcharPrecision(root.validatedRowType), true);

    // Apply CAST to the query tree
    final RelDataType relNodeRowTypeNullableMaxVarchar = typeFactory.createTypeWithNullability(
      typeFactory.createTypeWithMaxVarcharPrecision(root.rel.getRowType()), true);
    final RelNode relNodeWithCast = MoreRelOptUtil.createCastRel(root.rel, relNodeRowTypeNullableMaxVarchar);

    return RelRootPlus.of(root, relNodeWithCast, validatedRowTypeNullableMaxVarchar);
  }

  public static RelRoot expandView(NamespaceKey path,
                                   final CatalogIdentity viewOwner,
                                   final String queryString,
                                   final List<String> context,
                                   final SqlConverter sqlConverter,
                                   final BatchSchema batchSchema,
                                   VersionContext versionContext) {
    ViewExpansionToken token = null;

    try {
      token = sqlConverter.getViewExpansionContext().reserveViewExpansionToken(viewOwner);
      return getExpandedRelNode(path, viewOwner, queryString, context, sqlConverter, batchSchema, versionContext);
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof UserNotFoundException)) {
        throw e;
      }

      final CatalogIdentity delegatedUser = sqlConverter.getViewExpansionContext().getQueryUser();
      return getExpandedRelNode(path, delegatedUser, queryString, context, sqlConverter, batchSchema, versionContext);
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  private static RelNode wrapExpansionNode(SqlConverter sqlConverter, BatchSchema batchSchema, NamespaceKey path,
                                           RelRootPlus root, TableVersionContext versionContext) {
    List<String> vdsFields = batchSchema == null ?
      new ArrayList<>() :
      batchSchema.getFields().stream()
        .map(Field::getName)
        .sorted()
        .collect(Collectors.toList());
    return sqlConverter.getSubstitutionProvider().wrapExpansionNode(path, root.rel, vdsFields, root.validatedRowType,
      root.isContextSensitive(), versionContext, sqlConverter.getCatalog());
  }
}
