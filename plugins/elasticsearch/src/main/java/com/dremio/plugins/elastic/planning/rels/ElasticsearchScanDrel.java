/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rels;


import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.LogicalPlanImplementor;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.StoragePluginId;

public class ElasticsearchScanDrel extends ScanRelBase implements Rel {

  public ElasticsearchScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      StoragePluginId pluginId, TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment) {
    super(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ElasticsearchScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new ElasticsearchScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, projection, observedRowcountAdjustment);
  }

}

