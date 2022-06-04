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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.ops.ViewExpansionContext.ViewExpansionToken;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.collect.ImmutableList;

/**
 * An overridden implementation of SqlToRelConverter that redefines view expansion behavior.
 */
public class DremioSqlToRelConverter extends SqlToRelConverter {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSqlToRelConverter.class);

  private final SqlConverter sqlConverter;

  public DremioSqlToRelConverter(
      SqlConverter sqlConverter,
      SqlValidator validator,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(DremioToRelContext.createQueryContext(sqlConverter), validator,
        sqlConverter.getCatalogReader(), sqlConverter.getCluster(), convertletTable, config);
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
    final SqlConverter newConverter;
    if(viewOwner != null) {
      newConverter = sqlConverter.withSchemaPathAndUser(context, viewOwner);
    } else {
      newConverter = sqlConverter.withSchemaPath(context);
    }
    final SqlNode parsedNode = newConverter.parse(queryString);
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
