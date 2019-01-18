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
package com.dremio.exec.planner.acceleration;

import static com.dremio.exec.planner.logical.RelBuilder.newCalciteRelBuilderWithoutContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.dremio.exec.planner.StatelessRelShuttleImpl;
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

    protected String getRefreshColumn() {
      return refreshColumn;
    }

    public RelNode updateScan(IncrementallyUpdateable scan) {
      RelNode newScan = scan;
      RelDataTypeField refreshField = scan.getRowType().getField(refreshColumn, false, false);

      if (refreshField == null) {
        // Check if the field exist as part of the schema
        newScan = scan.projectInvisibleColumn(refreshColumn);
        if (newScan == null) {
          throw UserException.dataReadError()
              .message("Table does not include column identified for incremental update of name '%s'.", refreshColumn)
              .build(logger);
        }
        refreshField = newScan.getRowType().getField(refreshColumn, false, false);
      }

      if(refreshField.getType().getSqlTypeName() != SqlTypeName.BIGINT){
        throw UserException.dataReadError()
          .message("Dremio only supports incremental column update on BIGINT types. The identified column was of type %s.", refreshField.getType().getSqlTypeName())
          .build(logger);
      }

      // No need to add a project if field name is correct
      if (UPDATE_COLUMN.equals(refreshColumn)) {
        return newScan;
      }

      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(newScan.getCluster());

      relBuilder.push(newScan);

      List<String> newFieldNames = ImmutableList.<String>builder().addAll(newScan.getRowType().getFieldNames()).add(UPDATE_COLUMN).build();

      Iterable<RexInputRef> projects = Stream.concat(
          scan.getRowType().getFieldNames().stream().map(relBuilder::field),
          Stream.of(relBuilder.field(refreshColumn)))
          .collect(Collectors.toList());
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

    @Override
    public RelNode visit(TableScan tableScan) {
      if (!(tableScan instanceof IncrementallyUpdateable)) {
        return tableScan;
      }
      return updateScan((IncrementallyUpdateable) tableScan);
    }
  }

  /**
   * Abstract materialization implementation that updates an aggregate materialization plan to only add new data.
   */
  public static class MaterializationShuttle extends BaseShuttle {
    private final long value;

    public MaterializationShuttle(String refreshColumn, long value) {
      super(refreshColumn);
      this.value = value;
    }

    private RexNode generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory){
      return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    }

    @Override
    public RelNode visit(TableScan tableScan) {
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
}
