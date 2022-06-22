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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.ViewExpansionContext.ViewExpansionToken;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * An overridden implementation of SqlToRelConverter that redefines view expansion behavior.
 */
public class DremioSqlToRelConverter extends SqlToRelConverter {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSqlToRelConverter.class);

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
    return table.toRel(createToRelContext());
  }

  public ToRelContext createToRelContext() {
    return DremioToRelContext.createQueryContext(sqlConverter);
  }

  @Override
  public RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure) {
    RelStructuredTypeFlattener typeFlattener =
        new RelStructuredTypeFlattener(rexBuilder, createToRelContext(), restructure);
    return typeFlattener.rewrite(rootRel);
  }

  @Override
  protected RelRoot convertQueryRecursive(SqlNode query, boolean top, RelDataType targetRowType) {
    switch (query.getKind()) {
      case DELETE:
        LogicalTableModify logicalTableModify = (LogicalTableModify)(super.convertQueryRecursive(query, top, targetRowType).rel);
        Preconditions.checkNotNull(logicalTableModify);
        return RelRoot.of(TableModifyCrel.create(getTargetTable(query),  catalogReader, logicalTableModify.getInput(),
          LogicalTableModify.Operation.DELETE, null, null, false, null), query.getKind());
      case MERGE:
        return RelRoot.of(convertMerge((SqlMerge)query, getTargetTable(query)), query.getKind());
      case UPDATE:
        logicalTableModify = (LogicalTableModify)(super.convertQueryRecursive(query, top, targetRowType).rel);
        return RelRoot.of(TableModifyCrel.create(getTargetTable(query),  catalogReader, logicalTableModify.getInput(),
          LogicalTableModify.Operation.UPDATE, logicalTableModify.getUpdateColumnList(),
          logicalTableModify.getSourceExpressionList(), false, null), query.getKind());
      default:
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
      if (insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
        level2InsertExprs =
          ((LogicalProject) insertRel.getInput(0).getInput(0))
            .getProjects();
      }
      // Only include user columns (no extend columns)
      nLevel1Exprs = level1InsertExprs.size();
    }

    RelNode sourceInputRel = mergeSourceRel.getInput(0);
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
      LogicalTableModify.Operation.MERGE, null, null, false, targetColumnNameList);
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
      }
      else {
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
                                            final BatchSchema batchSchema) {
    SqlValidatorAndToRelContext.Builder builder = SqlValidatorAndToRelContext.builder(sqlConverter)
      .withSchemaPath(context);
    if(viewOwner != null) {
      builder = builder.withUser(viewOwner);
    }
    SqlValidatorAndToRelContext newConverter = builder.build();
    final SqlNode parsedNode = sqlConverter.parse(queryString);
    final SqlNode validatedNode = newConverter.validate(parsedNode);
    if (path != null && sqlConverter.getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
      final RelRootPlus unflattenedRoot = newConverter.toConvertibleRelRoot(validatedNode, true, false, false);
      ExpansionNode expansionNode = (ExpansionNode) wrapExpansionNode(
        sqlConverter,
        batchSchema,
        path,
        unflattenedRoot.rel,
        unflattenedRoot.validatedRowType,
        unflattenedRoot.isContextSensitive() || ExpansionNode.isContextSensitive(unflattenedRoot.rel));
      if (expansionNode.isDefault()) {
        sqlConverter.getFunctionContext().getContextInformation().setPlanCacheable(unflattenedRoot.isPlanCacheable());
        return new RelRoot(expansionNode, unflattenedRoot.validatedRowType, unflattenedRoot.kind,
          unflattenedRoot.fields, unflattenedRoot.collation, ImmutableList.of());
      }
    }
    final RelRootPlus root = newConverter.toConvertibleRelRoot(validatedNode, true, true);
    sqlConverter.getFunctionContext().getContextInformation().setPlanCacheable(root.isPlanCacheable());
    if(path == null) {
      return root;
    }

    // we need to make sure that if a inner expansion is context sensitive, we consider the current
    // expansion context sensitive even if it isn't locally.
    final boolean contextSensitive = root.isContextSensitive() || ExpansionNode.isContextSensitive(root.rel);

    return new RelRoot(ExpansionNode.wrap(path, root.rel, root.validatedRowType, contextSensitive, false),
      root.validatedRowType, root.kind, root.fields, root.collation, ImmutableList.of());
  }

  public static RelRoot expandView(NamespaceKey path, final CatalogIdentity viewOwner, final String queryString, final List<String> context, final SqlConverter sqlConverter, final BatchSchema batchSchema) {
    ViewExpansionToken token = null;

    try {
      token = sqlConverter.getViewExpansionContext().reserveViewExpansionToken(viewOwner);
      return getExpandedRelNode(path, viewOwner, queryString, context, sqlConverter, batchSchema);
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof UserNotFoundException)) {
        throw e;
      }

      final CatalogIdentity delegatedUser = sqlConverter.getViewExpansionContext().getQueryUser();
      return getExpandedRelNode(path, delegatedUser, queryString, context, sqlConverter, batchSchema);
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  private static RelNode wrapExpansionNode(SqlConverter sqlConverter, BatchSchema batchSchema, NamespaceKey path, RelNode root, RelDataType rowType, boolean contextSensitive) {
    List<String> vdsFields = batchSchema == null ?
      new ArrayList<>() :
      batchSchema.getFields().stream()
        .map(Field::getName)
        .sorted()
        .collect(Collectors.toList());
    return sqlConverter.getSubstitutionProvider().wrapExpansionNode(path, root, vdsFields, rowType, contextSensitive);
  }
}
