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
package com.dremio.exec.planner.acceleration;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.WarningTimer;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.acceleration.StrippingFactory.StripResult;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.descriptor.UnexpandedMaterializationDescriptor;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer;
import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer.Options;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.sql.DremioToRelContext;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NamespaceTable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/** Expander for materialization list. */
public class MaterializationExpander {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaterializationExpander.class);
  private final SqlConverter parent;
  private final CatalogService catalogService;

  private MaterializationExpander(final SqlConverter parent, final CatalogService catalogService) {
    this.parent = Preconditions.checkNotNull(parent, "parent is required");
    this.catalogService = catalogService;
  }

  public DremioMaterialization expand(UnexpandedMaterializationDescriptor descriptor) {
    try (WarningTimer timer =
        new WarningTimer(
            String.format(
                "Expand materialization descriptor %s/%s",
                descriptor.getLayoutId(), descriptor.getMaterializationId()),
            TimeUnit.SECONDS.toMillis(5))) {

      RelNode queryRel = deserializePlan(descriptor.getPlan(), parent, catalogService);

      final StrippingFactory factory =
          new StrippingFactory(parent.getSettings().getOptions(), parent.getConfig());
      StripResult stripResult =
          factory.strip(
              queryRel,
              descriptor.getReflectionType(),
              descriptor.getIncrementalUpdateSettings().isIncremental(),
              descriptor.getStripVersion());

      // if this is an incremental update, we need to do some changes to support the incremental.
      // These need to be applied after incremental update completes.
      final RelTransformer postStripNormalizer = getPostStripNormalizer(descriptor);
      stripResult = stripResult.transformNormalized(postStripNormalizer);

      logger.debug("Query rel:{}", RelOptUtil.toString(queryRel));

      RelNode tableRel = expandSchemaPath(descriptor.getPath());

      BatchSchema schema = ((ScanCrel) tableRel).getBatchSchema();
      final RelDataType strippedQueryRowType = stripResult.getNormalized().getRowType();
      tableRel = tableRel.accept(new IncrementalUpdateUtils.RemoveDirColumn(strippedQueryRowType));
      // Namespace table removes UPDATE_COLUMN from scans, but for incremental materializations, we
      // need to add it back
      // to the table scan
      if (descriptor.getIncrementalUpdateSettings().isIncremental()
          && !descriptor.getIncrementalUpdateSettings().isSnapshotBasedUpdate()) {
        tableRel = tableRel.accept(IncrementalUpdateUtils.ADD_MOD_TIME_SHUTTLE);
      }

      // if the row types don't match, ignoring the nullability, fail immediately
      if (!areRowTypesEqual(tableRel.getRowType(), strippedQueryRowType)) {
        throw new ExpansionException(
            String.format(
                "Materialization %s have different row types for its table and query rels.%n"
                    + "table row type %s%nquery row type %s",
                descriptor.getMaterializationId(),
                tableRel.getRowType().getFullTypeString(),
                strippedQueryRowType.getFullTypeString()));
      }

      try {
        // Check that the table rel row type matches that of the query rel,
        // if so, cast the table rel row types to the query rel row types.
        tableRel = MoreRelOptUtil.createCastRel(tableRel, strippedQueryRowType);
      } catch (Exception | AssertionError e) {
        throw UserException.planError(e)
            .message(
                "Failed to cast table rel row types to the query rel row types for materialization %s.%n"
                    + "table schema %s%nquery schema %s",
                descriptor.getMaterializationId(),
                CalciteArrowHelper.fromCalciteRowType(tableRel.getRowType()),
                CalciteArrowHelper.fromCalciteRowType(strippedQueryRowType))
            .build(logger);
      }

      // Wiping out RelMetadataCache. It will be holding the RelNodes from the prior
      // planning phases.
      queryRel.getCluster().invalidateMetadataQuery();

      return new DremioMaterialization(
          tableRel,
          queryRel,
          descriptor.getIncrementalUpdateSettings(),
          descriptor.getJoinDependencyProperties(),
          descriptor.getLayoutInfo(),
          descriptor.getMaterializationId(),
          schema,
          descriptor.getExpirationTimestamp(),
          descriptor
              .getStripVersion(), // Should use the strip version of the materialization we are
          // expanding
          postStripNormalizer);
    }
  }

  private final com.dremio.exec.planner.sql.handlers.RelTransformer getPostStripNormalizer(
      MaterializationDescriptor descriptor) {
    // for incremental update, we need to rewrite the queryRel so that it propagates the
    // UPDATE_COLUMN and
    // adds it as a grouping key in aggregates
    if (!descriptor.getIncrementalUpdateSettings().isIncremental()) {
      return com.dremio.exec.planner.sql.handlers.RelTransformer.NO_OP_TRANSFORMER;
    }
    final RelShuttle shuttle;
    if (descriptor.getIncrementalUpdateSettings().isSnapshotBasedUpdate()) {
      // For snapshot based incremental update, there is no UPDATE_COLUMN in plan. A DUMMY_COLUMN
      // ($_dremio_$_dummy_$)
      // needs to be added as a grouping key in aggregates.
      // This is to ensure built-in substitution rules to add proper roll up aggregates.
      shuttle = new IncrementalUpdateUtils.AddDummyGroupingFieldShuttle();
    } else {
      shuttle =
          Optional.ofNullable(descriptor.getIncrementalUpdateSettings().getUpdateField())
              .map(IncrementalUpdateUtils.SubstitutionShuttle::new)
              .orElse(IncrementalUpdateUtils.FILE_BASED_SUBSTITUTION_SHUTTLE);
    }
    return (rel) -> rel.accept(shuttle);
  }

  /**
   * Compare row types ignoring field names, nullability, ANY and CHAR/VARCHAR types. When
   * allowNullMismatch boolean is set, it allows INTEGER and NULL type match.
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
    final List<RelDataTypeField> f2 =
        rowType2.getFieldList(); // original materialization query field
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {

      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();

      Options options =
          Options.builder()
              .withConsiderNullability(false)
              .withConsiderPrecision(false)
              .withMatchAnyToAll(true)
              .build();
      if (RelDataTypeEqualityComparer.areEquals(type1, type2, options)) {
        continue;
      }

      if (type2.getSqlTypeName() == SqlTypeName.NULL
          && type1.getSqlTypeName() == SqlTypeName.INTEGER) {
        continue;
      }

      // are both types from the CHARACTER family ?
      if (type1.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER
          && type2.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
        continue;
      }

      // safely ignore when materialized field is DOUBLE instead of DECIMAL
      if (type1.getSqlTypeName() == SqlTypeName.DOUBLE
              && type2.getSqlTypeName() == SqlTypeName.DECIMAL
          || isSumAggOutput(type1, type2)) {
        continue;
      }

      return false;
    }

    return true;
  }

  private static boolean isSumAggOutput(RelDataType type1, RelDataType type2) {
    if (type1.getSqlTypeName() == SqlTypeName.DECIMAL
        && type2.getSqlTypeName() == SqlTypeName.DECIMAL) {
      // output of sum aggregation is always 38,inputScale
      return type1.getPrecision() == 38 && type1.getScale() == type2.getScale();
    }
    return false;
  }

  @VisibleForTesting
  RelNode expandSchemaPath(final List<String> path) {
    // TODO:  This can be simplified to not use DremioCatalogReader
    final DremioCatalogReader catalog = new DremioCatalogReader(parent.getPlannerCatalog());
    final RelOptTable table;
    try {
      table = catalog.getTable(path);
    } catch (RuntimeException e) {
      // Can occur if Iceberg table no longer exists or accelerator path changed
      throw new ExpansionException("Unable to get accelerator table: " + path, e);
    }

    if (table == null) {
      throw new ExpansionException("Unable to get accelerator table: " + path);
    }

    ToRelContext context = DremioToRelContext.createSerializationContext(parent.getCluster());

    NamespaceTable newTable = table.unwrap(NamespaceTable.class);
    if (newTable != null) {
      return newTable.toRel(context, table);
    }

    throw new ExpansionException("Unable to get accelerator table: " + path);
  }

  public static RelNode deserializePlan(
      final byte[] planBytes, SqlConverter parent, CatalogService catalogService) {
    final DremioCatalogReader dremioCatalogReader =
        new DremioCatalogReader(parent.getPlannerCatalog());
    try {
      final LogicalPlanDeserializer deserializer =
          parent
              .getSerializerFactory()
              .getDeserializer(
                  parent.getCluster(),
                  dremioCatalogReader,
                  DremioCompositeSqlOperatorTable.create(
                      parent.getFunctionImplementationRegistry(),
                      parent.getSettings().getOptions()),
                  catalogService);
      return deserializer.deserialize(planBytes);
    } catch (Exception ex) {
      try {
        // Try using legacy serializer. If this one also fails, throw the original exception.
        final LogicalPlanDeserializer deserializer =
            parent
                .getLegacySerializerFactory()
                .getDeserializer(
                    parent.getCluster(),
                    dremioCatalogReader,
                    DremioCompositeSqlOperatorTable.create(
                        parent.getFunctionImplementationRegistry(),
                        parent.getSettings().getOptions()),
                    catalogService);
        return deserializer.deserialize(planBytes);
      } catch (Exception ignored) {
        throw ex;
      }
    }
  }

  public static MaterializationExpander of(
      final SqlConverter parent, final CatalogService catalogService) {
    return new MaterializationExpander(parent, catalogService);
  }

  // Exceptions after successful deserialization
  public static class ExpansionException extends RuntimeException {
    public ExpansionException(String message) {
      super(message);
    }

    public ExpansionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  // Exceptions when regenerating the reflection plan
  public static class RebuildPlanException extends RuntimeException {
    public RebuildPlanException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
