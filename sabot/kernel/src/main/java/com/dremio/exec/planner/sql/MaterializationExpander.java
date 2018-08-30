/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.SubstitutionShuttle;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanDeserializer;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Expander for materialization list.
 */
public class MaterializationExpander {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializationExpander.class);
  private final SqlConverter parent;

  private MaterializationExpander(final SqlConverter parent) {
    this.parent = Preconditions.checkNotNull(parent, "parent is required");
  }

  public Optional<DremioRelOptMaterialization> expand(MaterializationDescriptor descriptor) {

    RelNode queryRel = deserializePlan(descriptor.getPlan(), parent);

    // for incremental update, we need to rewrite the queryRel so that it propogates the UPDATE_COLUMN and
    // adds it as a grouping key in aggregates
    if (descriptor.getIncrementalUpdateSettings().isIncremental()) {
      RelShuttle shuttle;
      if (descriptor.getIncrementalUpdateSettings().getUpdateField() == null) {
        shuttle = IncrementalUpdateUtils.FILE_BASED_SUBSTITUTION_SHUTTLE;
      } else {
        shuttle = new SubstitutionShuttle(descriptor.getIncrementalUpdateSettings().getUpdateField());
      }
      queryRel = queryRel.accept(shuttle);
    }

    logger.debug("Query rel:{}", RelOptUtil.toString(queryRel));

    RelNode tableRel = expandSchemaPath(descriptor.getPath());

    if (tableRel == null) {
      return Optional.empty();
    }

    BatchSchema schema = ((ScanCrel) tableRel).getBatchSchema();

    tableRel = tableRel.accept(new IncrementalUpdateUtils.RemoveDirColumn(queryRel.getRowType()));

    // Namespace table removes UPDATE_COLUMN from scans, but for incremental materializations, we need to add it back
    // to the table scan
    if (descriptor.getIncrementalUpdateSettings().isIncremental()) {
      tableRel = tableRel.accept(IncrementalUpdateUtils.ADD_MOD_TIME_SHUTTLE);
    }

    // if the row types don't match, ignoring the nullability, fail immediately
    if (!areRowTypesEqual(tableRel.getRowType(), queryRel.getRowType())) {
      throw new ExpansionException(String.format("Materialization %s have different row types for its table and query rels.%n" +
        "table row type %s%nquery row type %s", descriptor.getMaterializationId(), tableRel.getRowType(), queryRel.getRowType()));
    }

    try {
      // Check that the table rel row type matches that of the query rel,
      // if so, cast the table rel row types to the query rel row types.
      tableRel = MoreRelOptUtil.createCastRel(tableRel, queryRel.getRowType());
    } catch (Exception | AssertionError e) {
      throw UserException.planError(e)
        .message("Failed to cast table rel row types to the query rel row types for materialization %s.%n" +
          "table schema %s%nquery schema %s", descriptor.getMaterializationId(),
          BatchSchema.fromCalciteRowType(tableRel.getRowType()),
          BatchSchema.fromCalciteRowType(queryRel.getRowType()))
        .build(logger);
    }

    return Optional.of(new DremioRelOptMaterialization(
      tableRel,
      queryRel,
      descriptor.getIncrementalUpdateSettings(),
      descriptor.getJoinDependencyProperties(),
      descriptor.getLayoutInfo(),
      descriptor.getMaterializationId(),
      schema,
      descriptor.getExpirationTimestamp()
    ));
  }

  /**
   * Compare row types ignoring field names, nullability, ANY and CHAR/VARCHAR types
   */
  @VisibleForTesting
  static boolean areRowTypesEqual(RelDataType rowType1, RelDataType rowType2) {
      if (rowType1 == rowType2) {
        return true;
      }

      if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
        return false;
      }

      final List<RelDataTypeField> f1 = rowType1.getFieldList(); // materialized field
      final List<RelDataTypeField> f2 = rowType2.getFieldList(); // original materialization query field
      for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
        // remove nullability
        final RelDataType type1 = JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(pair.left.getType(), false);
        final RelDataType type2 = JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(pair.right.getType(), false);

        // are types equal ?
        if (type1.equals(type2)) {
          continue;
        }

        // ignore ANY types
        if (type1.getSqlTypeName() == SqlTypeName.ANY || type2.getSqlTypeName() == SqlTypeName.ANY) {
          continue;
        }

        // are both types from the CHARACTER family ?
        if (type1.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER &&
            type2.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          continue;
        }

        // safely ignore when materialized field is DOUBLE instead of DECIMAL
        if (type1.getSqlTypeName() == SqlTypeName.DOUBLE && type2.getSqlTypeName() == SqlTypeName.DECIMAL) {
          continue;
        }

        return false;
      }

      return true;
  }

  private RelNode expandSchemaPath(final List<String> path) {
    final DremioCatalogReader catalog = parent.getCatalogReader();
    final RelOptTable table = catalog.getTable(path);
    if(table == null){
      return null;
    }

    ToRelContext context = new ToRelContext() {
      @Override
      public RelOptCluster getCluster() {
        return parent.getCluster();
      }

      @Override
      public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                List<String> viewPath) {
        return null;
      }
    };

    NamespaceTable newTable = table.unwrap(NamespaceTable.class);
    if(newTable != null){
      return newTable.toRel(context, table);
    }

    throw new IllegalStateException("Unable to expand path for table: " + table);
  }

  public static RelNode deserializePlan(final byte[] planBytes, SqlConverter parent) {
    final SqlConverter parser = new SqlConverter(parent, parent.getCatalogReader().withSchemaPath(ImmutableList.of()));

    final LogicalPlanDeserializer deserializer = KryoLogicalPlanSerializers.forDeserialization(parser.getCluster(), parser.getCatalogReader());
    return deserializer.deserialize(planBytes);
  }

  public static MaterializationExpander of(final SqlConverter parent) {
    return new MaterializationExpander(parent);
  }

  public static class ExpansionException extends RuntimeException {
    ExpansionException(String message) {
      super(message);
    }
  }

}
