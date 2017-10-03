/*
 * Copyright (C) 2017 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.exec.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * A StatelessRelShuttle that routes calls back to the generic visit(RelNode method)
 */
public abstract class RoutingShuttle extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    return visit((RelNode) aggregate);
  }

  @Override
  public RelNode visit(TableScan scan) {
    return visit((RelNode) scan);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    return visit((RelNode) scan);
  }

  @Override
  public RelNode visit(LogicalValues values) {
    return visit((RelNode) values);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    return visit((RelNode) filter);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    return visit((RelNode) project);
  }

  @Override
  public RelNode visit(RelNode other) {
    return super.visit(other);
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    return visit((RelNode) join);
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    return visit((RelNode) correlate);
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    return visit((RelNode) union);
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    return visit((RelNode) intersect);
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    return visit((RelNode) minus);
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    return visit((RelNode) sort);
  }

  @Override
  public RelNode visit(LogicalExchange exchange) {
    return visit((RelNode) exchange);
  }

}
