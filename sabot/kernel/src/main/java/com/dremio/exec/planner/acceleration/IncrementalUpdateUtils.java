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

import static com.dremio.exec.planner.logical.RelBuilder.newCalciteRelBuilderWithoutContext;

import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.ConvertibleScan;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Contains various utilities for acceleration incremental updates
 */
public class IncrementalUpdateUtils {
  private static final Logger logger = LoggerFactory.getLogger(IncrementalUpdateUtils.class);

  public static final String UPDATE_COLUMN = "$_dremio_$_update_$";

  public static final SubstitutionShuttle FILE_BASED_SUBSTITUTION_SHUTTLE = new SubstitutionShuttle(UPDATE_COLUMN);

  public abstract static class BaseShuttle extends StatelessRelShuttleImpl {
    private final String refreshColumn;

    public BaseShuttle(String refreshColumn) {
      this.refreshColumn = refreshColumn;
    }

    public RelNode updateScan(IncrementallyUpdateable scan) {
      if (UPDATE_COLUMN.equals(refreshColumn)) {
        return scan;
      }

      RelDataTypeField refreshField = scan.getRowType().getField(refreshColumn, false, false);

      if(refreshField == null){
        throw UserException.dataReadError()
          .message("Table does not include column identified for incremental update of name '%s'.", refreshField.getName())
          .build(logger);
      } else if(refreshField.getType().getSqlTypeName() != SqlTypeName.BIGINT){
        throw UserException.dataReadError()
          .message("Dremio only supports incremental column update on BIGINT types. The identified column was of type %s.", refreshField.getType().getSqlTypeName())
          .build(logger);
      }

      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(scan.getCluster());

      relBuilder.push(scan);

      List<String> newFieldNames = ImmutableList.<String>builder().addAll(scan.getRowType().getFieldNames()).add(UPDATE_COLUMN).build();

      Iterable<RexInputRef> projects = FluentIterable.from(scan.getRowType().getFieldNames())
        .transform(new Function<String, RexInputRef>() {
          @Override
          public RexInputRef apply(String fieldName) {
            return relBuilder.field(fieldName);
          }
        })
        .append(relBuilder.field(refreshColumn));

      relBuilder.project(projects, newFieldNames);

      return relBuilder.build();
    }


    @Override
    public RelNode visit(final LogicalFilter filter) {
      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(filter.getCluster());
      RelNode input = filter.getInput().accept(this);
      relBuilder.push(input);

      RexNode newCondition = filter.getCondition().accept(new RexShuttle() {
        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
          return relBuilder.field(filter.getRowType().getFieldNames().get(inputRef.getIndex()));
        }
      });

      relBuilder.filter(newCondition);
      return relBuilder.build();
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

  /**
   * Rewrites the plan associated with a materialization such that it contains the $updateId. For column based incremental updates,
   * the updateId corresponds to an indicated column, which must be strictly increasing.
   */
  public static class SubstitutionShuttle extends BaseShuttle {

    public SubstitutionShuttle(String refreshColumn) {
      super(refreshColumn);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode input = aggregate.getInput().accept(this);


      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(aggregate.getCluster());
      relBuilder.push(input);

      RelDataType incomingRowType = input.getRowType();
      RelDataTypeField modField = incomingRowType.getField(UPDATE_COLUMN, false, false);
      ImmutableBitSet newGroupSet = aggregate.getGroupSet().rebuild().set(modField.getIndex()).build();
      GroupKey groupKey = relBuilder.groupKey(newGroupSet, aggregate.indicator, null);

      relBuilder.aggregate(groupKey, aggregate.getAggCallList());

      Iterable<RexInputRef> projects = FluentIterable.from(aggregate.getRowType().getFieldNames())
        .transform(new Function<String, RexInputRef>() {
          @Override
          public RexInputRef apply(String fieldName) {
            return relBuilder.field(fieldName);
          }
        })
        .append(relBuilder.field(UPDATE_COLUMN));

      relBuilder.project(projects);

      return relBuilder.build();
    }
  }

  /**
   * Abstract materialization implementation that updates an aggregate materialization plan to only add new data.
   */
  private abstract static class MaterializationShuttle extends BaseShuttle {

    public MaterializationShuttle(String refreshColumn) {
      super(refreshColumn);
    }

    abstract RexNode generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory);

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

  /**
   * Visitor that checks if a logical plan can support incremental update. The supported pattern right now is a plan
   * that contains only Filters, Projects, Scans, and Aggregates. There can only be one Aggregate in the plan, and the
   * Scan most support incremental update.
   */
  public static class IncrementalChecker extends RoutingShuttle {
    private final NamespaceService namespaceService;

    private RelNode unsupportedOperator = null;
    private boolean isIncremental = false;
    private int aggCount = 0;

    public IncrementalChecker(NamespaceService namespaceService) {
      this.namespaceService = namespaceService;
    }

    public boolean isIncremental() {
      if (!isIncremental) {
        logger.debug("Cannot do incremental update because the table is not incrementally updateable");
        return false;
      }

      if (unsupportedOperator != null) {
        logger.debug("Cannot do incremental update because {} does not support incremental update", unsupportedOperator.getRelTypeName());
        return false;
      }

      if (aggCount > 1) {
        logger.debug("Cannot do incremental update because has multiple aggregate operators");
        return false;
      }

      return true;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (unsupportedOperator == null) {
        unsupportedOperator = other;
      }
      return other;
    }

    @Override
    public RelNode visit(TableScan tableScan) {
      List<String> tablePath = tableScan.getTable().getQualifiedName();
      try {
        boolean tableIsIncremental = namespaceService.getDataset(new NamespaceKey(tablePath)).getPhysicalDataset().getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL;
        isIncremental = tableIsIncremental;
      } catch (NamespaceException e) {
        isIncremental = false;
      }
      return tableScan;
    }

    public RelNode visit(LogicalAggregate aggregate) {
      aggCount++;
      return visitChild(aggregate, 0, aggregate.getInput());
    }

    @Override
    public RelNode visit(LogicalProject project) {
      return visitChild(project, 0, project.getInput());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      return visitChild(filter, 0, filter.getInput());
    }
  }

  /**
   * Check if a plan can support incremental update
   * @param plan
   * @param namespaceService
   * @return
   */
  public static boolean getIncremental(RelNode plan, final NamespaceService namespaceService) {
    IncrementalChecker checker = new IncrementalChecker(namespaceService);
    plan.accept(checker);
    return checker.isIncremental();
  }

  public static String findRefreshField(RelNode plan, final NamespaceService namespaceService) {
    final Pointer<String> refreshField = new Pointer<>();
    plan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(TableScan tableScan) {
        List<String> tablePath = tableScan.getTable().getQualifiedName();
        try {
          String field = namespaceService.getDataset(new NamespaceKey(tablePath)).getPhysicalDataset().getAccelerationSettings().getRefreshField();
          refreshField.value = field;
        } catch (NamespaceException e) {
          throw new RuntimeException(e);
        }
        return tableScan;
      }
    });
    return refreshField.value;
  }

  public static class ColumnMaterializationShuttle extends MaterializationShuttle {

    private final long value;

    public ColumnMaterializationShuttle(String refreshColumn, long value) {
      super(refreshColumn);
      this.value = value;
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
      super(UPDATE_COLUMN);
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
