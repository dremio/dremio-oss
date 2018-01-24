/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.SubstitutionShuttle;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;

import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanDeserializer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Expander for materialization list.
 */
public class MaterializationExpander {
  private final SqlConverter parent;

  private MaterializationExpander(final SqlConverter parent) {
    this.parent = Preconditions.checkNotNull(parent, "parent is required");
  }

  public Optional<DremioRelOptMaterialization> expand(MaterializationDescriptor descriptor) {

    final RelNode deserializedPlan = deserializePlan(descriptor.getPlan());

    RelNode queryRel = deserializedPlan;

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


    RelNode tableRel = expandSchemaPath(descriptor.getPath());

    if (tableRel == null) {
      return Optional.absent();
    }

    BatchSchema schema = ((ScanCrel) tableRel).getBatchSchema();

    tableRel = tableRel.accept(new IncrementalUpdateUtils.RemoveDirColumn(queryRel.getRowType()));

    // Namespace table removes UPDATE_COLUMN from scans, but for incremental materializations, we need to add it back
    // to the table scan
    if (descriptor.getIncrementalUpdateSettings().isIncremental()) {
      tableRel = tableRel.accept(IncrementalUpdateUtils.ADD_MOD_TIME_SHUTTLE);
    }

    // Check that the table rel row type matches that of the query rel,
    // if so, cast the table rel row types to the query rel row types.
    tableRel = MoreRelOptUtil.createCastRel(tableRel, queryRel.getRowType());

    return Optional.of(new DremioRelOptMaterialization(
      tableRel,
      queryRel,
      descriptor.getIncrementalUpdateSettings(),
      descriptor.getLayoutInfo(),
      descriptor.getMaterializationId(),
      schema,
      descriptor.getExpirationTimestamp()
    ));
  }

  private RelNode expandSchemaPath(final List<String> path) {
    final CalciteCatalogReader catalog = parent.getCatalog();
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

      @Override
      public RelRoot expandView(RelDataType rowType, String queryString, SchemaPlus rootSchema, List<String> schemaPath,
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

  private RelNode deserializePlan(final byte[] planBytes) {
    final SqlConverter parser = new SqlConverter(parent, parent.getDefaultSchema(), parent.getRootSchema(), parent.getCatalog().withSchemaPath(ImmutableList.<String>of()));
    final LogicalPlanDeserializer deserializer = KryoLogicalPlanSerializers.forDeserialization(parser.getCluster(),
      parser.getCatalog(), parser.getPluginRegistry());
    return deserializer.deserialize(planBytes);
  }

  public static MaterializationExpander of(final SqlConverter parent) {
    return new MaterializationExpander(parent);
  }

}
