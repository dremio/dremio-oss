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
package com.dremio.exec.planner.acceleration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.ConvertibleScan;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Contains various utilities for acceleration incremental updates
 */
public class IncrementalUpdateUtils {
  private static final Logger logger = LoggerFactory.getLogger(IncrementalUpdateUtils.class);

  public static final String UPDATE_COLUMN = "$_dremio_$_update_$";

  /**
   * Abstract materialization implementation that updates an aggregate materialization plan to only add new data.
   */
  private abstract static class MaterializationShuttle extends StatelessRelShuttleImpl {

    abstract RexNode generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory);

    abstract RelNode updateScan(IncrementallyUpdateable updateable);

    @Override
    public RelNode visit(TableScan tableScan) {
      if (tableScan instanceof ConvertibleScan) {
        return ((ConvertibleScan) tableScan).convert().accept(this);
      }
      if (!(tableScan instanceof IncrementallyUpdateable)) {
        return tableScan;
      }

      final RelNode newScan = updateScan((IncrementallyUpdateable) tableScan);

      // build new filter to apply refresh condition.
      final RexBuilder rexBuilder = tableScan.getCluster().getRexBuilder();
      final RexNode inputRef = rexBuilder.makeInputRef(newScan, newScan.getRowType().getField(UPDATE_COLUMN, false, false).getIndex());
      final RexNode literal = generateLiteral(rexBuilder, tableScan.getCluster().getTypeFactory());
      final RexNode condition = tableScan.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.GREATER_THAN, ImmutableList.of(inputRef, literal));
      return LogicalFilter.create(newScan, condition);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode input = aggregate.getInput().accept(this);
      RelDataType incomingRowType = input.getRowType();
      RelDataTypeField modField = incomingRowType.getField(UPDATE_COLUMN, false, false);
      if (modField == null) {
        return aggregate;
      }

      final AggregateCall aggCall = AggregateCall.create(SqlStdOperatorTable.MAX, false, ImmutableList.of(modField.getIndex()), -1, modField.getType(), UPDATE_COLUMN);
      final List<AggregateCall> aggCalls = FluentIterable.from(aggregate.getAggCallList())
        .append(aggCall)
        .toList();
      return aggregate.copy(
        aggregate.getTraitSet(),
        input,
        aggregate.indicator,
        aggregate.getGroupSet(),
        null,
        aggCalls
      );
    }

    @Override
    public RelNode visit(LogicalProject project) {
      RelNode input = project.getInput().accept(this);
      RelDataType incomingRowType = input.getRowType();
      List<RexNode> newProjects;
      RelDataTypeField modField = incomingRowType.getField(UPDATE_COLUMN, false, false);
      if (modField == null) {
        return project;
      }
      newProjects = FluentIterable.from(project.getProjects())
        .append(new RexInputRef(modField.getIndex(), modField.getType()))
        .toList();
      FieldInfoBuilder fieldInfoBuilder = new FieldInfoBuilder(project.getCluster().getTypeFactory());
      for (RelDataTypeField field : project.getRowType().getFieldList()) {
        fieldInfoBuilder.add(field);
      }
      fieldInfoBuilder.add(UPDATE_COLUMN, modField.getType());
      return new LogicalProject(
        project.getCluster(),
        project.getTraitSet(),
        input,
        newProjects,
        fieldInfoBuilder.build()
      );
    }
  }

  public static final AddModTimeShuttle ADD_MOD_TIME_SHUTTLE = new AddModTimeShuttle();

  public static class AddModTimeShuttle extends StatelessRelShuttleImpl {
    @Override
    public RelNode visit(TableScan tableScan) {
      if (tableScan instanceof ConvertibleScan) {
        return ((ConvertibleScan) tableScan).convert().accept(this);
      }

      if(tableScan instanceof IncrementallyUpdateable){
        return ((IncrementallyUpdateable) tableScan).projectInvisibleColumn(UPDATE_COLUMN);
      }

      return tableScan;
    }
  }

  public static class RemoveDirColumn extends StatelessRelShuttleImpl {
    private final Set<String> columns;

    public RemoveDirColumn(RelDataType rowType) {
      columns = FluentIterable.from(rowType.getFieldNames()).toSet();
    }

    @Override
    public RelNode visit(TableScan tableScan) {
      if(tableScan instanceof IncrementallyUpdateable){
        return ((IncrementallyUpdateable) tableScan).filterColumns(new Predicate<String>(){
          @Override
          public boolean apply(String field) {
            return columns.contains(field);
          }});
      }
      return tableScan;
    }
  }

  public static boolean getIncremental(RelNode plan, final NamespaceService namespaceService) {
    final AtomicReference<Boolean> isIncremental = new AtomicReference<>(false);
    final AtomicReference<Boolean> hasJoin = new AtomicReference<>(false);
    plan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(TableScan tableScan) {
        List<String> tablePath = tableScan.getTable().getQualifiedName();
        try {
          boolean tableIsIncremental = namespaceService.getDataset(new NamespaceKey(tablePath)).getPhysicalDataset().getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL;
          isIncremental.set(tableIsIncremental);
        } catch (NamespaceException e) {
          isIncremental.set(false);
        }
        return tableScan;
      }

      @Override
      public RelNode visit(LogicalJoin logicalJoin) {
        hasJoin.set(true);
        return logicalJoin;
      }
    });
    return isIncremental.get() && !hasJoin.get();
  }

  public static String findRefreshField(RelNode plan, final NamespaceService namespaceService) {
    final AtomicReference<String> refreshField = new AtomicReference<>();
    plan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(TableScan tableScan) {
        List<String> tablePath = tableScan.getTable().getQualifiedName();
        try {
          String field = namespaceService.getDataset(new NamespaceKey(tablePath)).getPhysicalDataset().getAccelerationSettings().getRefreshField();
          refreshField.set(field);
        } catch (NamespaceException e) {
          throw new RuntimeException(e);
        }
        return tableScan;
      }
    });
    return refreshField.get();
  }

  public static class ColumnMaterializationShuttle extends MaterializationShuttle {

    private final String refreshColumn;
    private final long value;

    public ColumnMaterializationShuttle(String refreshColumn, long value) {
      this.refreshColumn = refreshColumn;
      this.value = value;
    }

    public RelNode updateScan(IncrementallyUpdateable updateable){
      final FieldInfoBuilder rowTypeBuilder = new FieldInfoBuilder(updateable.getCluster().getTypeFactory());
      final RexBuilder rexBuilder = updateable.getCluster().getRexBuilder();
      final List<RexNode> projects = new ArrayList<>();

      projects.addAll(FluentIterable.from(updateable.getRowType().getFieldList())
          .transform(new Function<RelDataTypeField, RexNode>(){
            @Override
            public RexNode apply(RelDataTypeField input) {
              rowTypeBuilder.add(input);
              return rexBuilder.makeInputRef(input.getType(), input.getIndex());
            }}).toList());
      RelDataTypeField refreshField = updateable.getRowType().getField(refreshColumn, false, false);
      if(refreshField == null){
        throw UserException.dataReadError()
          .message("Table does not include column identified for incremental update of name '%s'.", refreshField.getName())
          .build(logger);
      } else if(refreshField.getType().getSqlTypeName() != SqlTypeName.BIGINT){
        throw UserException.dataReadError()
          .message("Dremio only supports incremental column update on BIGINT types. The identified column was of type %s.", refreshField.getType().getSqlTypeName())
          .build(logger);
      }
      projects.add(rexBuilder.makeInputRef(refreshField.getType(), refreshField.getIndex()));
      rowTypeBuilder.add(UPDATE_COLUMN, refreshField.getType());
      return new LogicalProject(updateable.getCluster(), updateable.getTraitSet(), updateable, projects, rowTypeBuilder.build());
    }

    public RexNode generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory){
      return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    }
  }

  /**
   * For file-based incrementally updated accelerations, the plan for updating materializations is rewritten to add the $updateId
   * field to the tableScan, and also add a filter to only select files that have a modification time later than the last
   * updates max($updateId). Aggregations are also modified to include max($updateId) as $updateId.
   */
  public static class FileMaterializationShuttle extends MaterializationShuttle {

    private final long timeStamp;

    public FileMaterializationShuttle(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public RelNode updateScan(IncrementallyUpdateable updateable){
      return updateable.projectInvisibleColumn(UPDATE_COLUMN);
    }

    public RexNode generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory){
      return rexBuilder.makeLiteral(timeStamp, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    }
  }
}
