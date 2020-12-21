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

import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
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
import com.dremio.exec.planner.acceleration.StrippingFactory.StripResult;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
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

  public DremioMaterialization expand(MaterializationDescriptor descriptor) {

    RelNode queryRel = deserializePlan(descriptor.getPlan(), parent);

    // used for old reflections where we stripped before plan persistence.
    final boolean preStripped = descriptor.getStrippedPlanHash() == null;
    final StrippingFactory factory = new StrippingFactory(parent.getSettings().getOptions(), parent.getConfig());

    StripResult stripResult = preStripped ? StrippingFactory.noStrip(queryRel) : factory.strip(queryRel, descriptor.getReflectionType(), descriptor.getIncrementalUpdateSettings().isIncremental(), descriptor.getStripVersion());

    // we need to make sure that the persisted version of the plan after applying the stripping is
    // consistent with what we got when materializing. We'll do this again during substitution in
    // various forms and are doing it here for checking the validity of the expansion.
    if(!preStripped) {
      long strippedHash = PlanHasher.hash(stripResult.getNormalized());
      if(strippedHash != descriptor.getStrippedPlanHash()) {
        throw new ExpansionException(String.format("Stripped hash doesn't match expect stripped hash. Stripped logic likely changed. Non-matching plan: %s.", RelOptUtil.toString(stripResult.getNormalized())));
      }
    }

    // if this is an incremental update, we need to do some changes to support the incremental. These need to be applied after incremental update completes.
    final RelTransformer postStripNormalizer = getPostStripNormalizer(descriptor);
    stripResult = stripResult.transformNormalized(postStripNormalizer);

    logger.debug("Query rel:{}", RelOptUtil.toString(queryRel));

    RelNode tableRel = expandSchemaPath(descriptor.getPath());

    if (tableRel == null) {
      throw new ExpansionException("Unable to find read metadata for materialization.");
    }

    BatchSchema schema = ((ScanCrel) tableRel).getBatchSchema();
    final RelDataType strippedQueryRowType = stripResult.getNormalized().getRowType();
    tableRel = tableRel.accept(new IncrementalUpdateUtils.RemoveDirColumn(strippedQueryRowType));

    // Namespace table removes UPDATE_COLUMN from scans, but for incremental materializations, we need to add it back
    // to the table scan
    if (descriptor.getIncrementalUpdateSettings().isIncremental()) {
      tableRel = tableRel.accept(IncrementalUpdateUtils.ADD_MOD_TIME_SHUTTLE);
    }

    // if the row types don't match, ignoring the nullability, fail immediately
    if (!areRowTypesEqual(tableRel.getRowType(), strippedQueryRowType)) {
      throw new ExpansionException(String.format("Materialization %s have different row types for its table and query rels.%n" +
        "table row type %s%nquery row type %s", descriptor.getMaterializationId(), tableRel.getRowType(), strippedQueryRowType));
    }

    try {
      // Check that the table rel row type matches that of the query rel,
      // if so, cast the table rel row types to the query rel row types.
      tableRel = MoreRelOptUtil.createCastRel(tableRel, strippedQueryRowType);
    } catch (Exception | AssertionError e) {
      throw UserException.planError(e)
        .message("Failed to cast table rel row types to the query rel row types for materialization %s.%n" +
          "table schema %s%nquery schema %s", descriptor.getMaterializationId(),
          CalciteArrowHelper.fromCalciteRowType(tableRel.getRowType()),
          CalciteArrowHelper.fromCalciteRowType(strippedQueryRowType))
        .build(logger);
    }

    return new DremioMaterialization(
      tableRel,
      queryRel,
      descriptor.getIncrementalUpdateSettings(),
      descriptor.getJoinDependencyProperties(),
      descriptor.getLayoutInfo(),
      descriptor.getMaterializationId(),
      schema,
      descriptor.getExpirationTimestamp(),
      preStripped,
      StrippingFactory.LATEST_STRIP_VERSION,
      postStripNormalizer
    );
  }

  private final com.dremio.exec.planner.sql.handlers.RelTransformer getPostStripNormalizer(MaterializationDescriptor descriptor) {
    // for incremental update, we need to rewrite the queryRel so that it propagates the UPDATE_COLUMN and
    // adds it as a grouping key in aggregates
    if (!descriptor.getIncrementalUpdateSettings().isIncremental()) {
      return com.dremio.exec.planner.sql.handlers.RelTransformer.NO_OP_TRANSFORMER;
    }

    final RelShuttle shuttle = Optional.ofNullable(descriptor.getIncrementalUpdateSettings().getUpdateField())
        .map(IncrementalUpdateUtils.SubstitutionShuttle::new)
        .orElse(IncrementalUpdateUtils.FILE_BASED_SUBSTITUTION_SHUTTLE);
    return (rel) -> rel.accept(shuttle);
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
        if (type1.getSqlTypeName() == SqlTypeName.DOUBLE && type2.getSqlTypeName() == SqlTypeName
          .DECIMAL || isSumAggOutput(type1, type2)) {
          continue;
        }

        return false;
      }

      return true;
  }

  private static boolean isSumAggOutput(RelDataType type1, RelDataType type2) {
    if (type1.getSqlTypeName() == SqlTypeName
      .DECIMAL && type2.getSqlTypeName() == SqlTypeName.DECIMAL) {
      // output of sum aggregation is always 38,inputScale
      return type1.getPrecision() == 38 && type1.getScale() == type2.getScale();
    }
    return false;
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
    final LogicalPlanDeserializer deserializer = parser.getSerializerFactory().getDeserializer(parser.getCluster(), parser.getCatalogReader(), parser.getFunctionImplementationRegistry());
    return deserializer.deserialize(planBytes);
  }

  public static MaterializationExpander of(final SqlConverter parent) {
    return new MaterializationExpander(parent);
  }

  public static class ExpansionException extends RuntimeException {
    public ExpansionException(String message) {
      super(message);
    }
  }

  private static RelNode applyRule(RelNode node, RelOptRule rule) {
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addMatchOrder(HepMatchOrder.ARBITRARY);
    builder.addRuleCollection(ImmutableList.of(rule));
    final HepProgram program = builder.build();

    final HepPlanner planner = new HepPlanner(program);
    planner.setRoot(node);
    return planner.findBestExp();
  }

}
