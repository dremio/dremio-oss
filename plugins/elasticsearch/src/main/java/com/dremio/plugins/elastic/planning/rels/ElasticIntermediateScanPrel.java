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
package com.dremio.plugins.elastic.planning.rels;

import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSpecialType;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticTableXattr;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.elastic.mapping.FieldAnnotation;
import com.dremio.plugins.elastic.planning.rules.SchemaField;
import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

public class ElasticIntermediateScanPrel extends ScanPrelBase implements ElasticsearchPrel {

  private Map<SchemaPath, FieldAnnotation> annotations;
  private ElasticTableXattr extendedAttributes;

  public ElasticIntermediateScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      List<Info> runtimeFilters) {
    super(
        cluster,
        traits(cluster, table.getRowCount(), dataset.getSplitCount(), traitSet),
        table,
        dataset.getStoragePluginId(),
        dataset,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        runtimeFilters);
  }

  private static RelTraitSet traits(
      RelOptCluster cluster, double rowCount, int splitCount, RelTraitSet traitSet) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
    boolean smallInput = rowCount < settings.getSliceTarget();

    DistributionTrait distribution;
    if (settings.isMultiPhaseAggEnabled()
        && !settings.isSingleMode()
        && !smallInput
        && splitCount > 1) {
      distribution = DistributionTrait.ANY;
    } else {
      distribution = DistributionTrait.SINGLETON;
    }

    return traitSet.plus(distribution);
  }

  @Override
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public FieldAnnotation getAnnotation(SchemaPath path) {
    if (annotations == null) {
      annotations = FieldAnnotation.getAnnotationMap(getExtendedAttributes().getAnnotationList());
    }

    return annotations.get(path);
  }

  public ElasticTableXattr getExtendedAttributes() {
    if (extendedAttributes == null) {
      try {
        extendedAttributes =
            ElasticTableXattr.parseFrom(
                tableMetadata.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
      } catch (InvalidProtocolBufferException e) {
        throw Throwables.propagate(e);
      }
    }

    return extendedAttributes;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ElasticIntermediateScanPrel(
        getCluster(),
        traitSet,
        getTable(),
        tableMetadata,
        getProjectedColumns(),
        observedRowcountAdjustment,
        hints,
        getRuntimeFilters());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator physicalPlanCreator)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ElasticIntermediateScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new ElasticIntermediateScanPrel(
        getCluster(),
        getTraitSet(),
        table,
        tableMetadata,
        projection,
        observedRowcountAdjustment,
        hints,
        getRuntimeFilters());
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    return tableMetadata.getSchema().maskAndReorder(getProjectedColumns());
  }

  @Override
  public ScanBuilder newScanBuilder() {
    return new ScanBuilder();
  }

  public SchemaPath getDirectReferenceIfPossible(RexNode expression, IndexMode indexMode) {
    return expression.accept(new SchemaField.PathVisitor(getRowType(), indexMode));
  }

  public ElasticSpecialType getSpecialTypeRecursive(SchemaPath path) {
    while (path != null) {
      FieldAnnotation a = getAnnotation(path);
      if (a != null) {
        if (a.hasSpecialType()) {
          return a.getSpecialType();
        }
      }
      path = path.getParent();
    }

    return null;
  }

  public FieldAnnotation getAnnotation(RexNode expression, IndexMode indexMode) {
    // given an expression, determine if it has an annotation (meaning it is a direct derivative and
    // has an annotation.

    SchemaPath path = getDirectReferenceIfPossible(expression, indexMode);
    if (path == null) {
      // not a direct reference.
      return null;
    }

    return getAnnotation(path);
  }

  /** How to handle numer indices when building a schema path. */
  public static enum IndexMode {
    /** Construct a path by skipping numeric dereferences. */
    SKIP,

    /** Don't allow constructing paths that have index references. */
    DISALLOW,

    /**
     * Allow constructing paths that have index references and return them in the built SchemaPath.
     */
    ALLOW
  }
}
