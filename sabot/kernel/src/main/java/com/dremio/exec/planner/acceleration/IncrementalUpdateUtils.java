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

import static com.dremio.exec.planner.logical.RelBuilder.newCalciteRelBuilderWithoutContext;

import java.util.List;
import java.util.Optional;
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
import com.dremio.common.types.MinorType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.Pointer;
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

      switch(refreshField.getType().getSqlTypeName()) {
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case VARCHAR:
        case TIMESTAMP:
        case DATE:
        case DECIMAL:
          break;
        default:
          throw UserException.dataReadError()
              .message("Dremio only supports incremental column update on INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, TIMESTAMP, DATE, and DECIMAL types. The identified column was of type %s.", refreshField.getType().getSqlTypeName())
              .build(logger);
      }

      // No need to add a project if field name is correct
      if (UPDATE_COLUMN.equals(refreshColumn)) {
        return newScan;
      }

      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(newScan.getCluster());

      relBuilder.push(newScan);

      List<String> newFieldNames = ImmutableList.<String>builder().addAll(newScan.getRowType().getFieldNames()).add(UPDATE_COLUMN).build();

      Iterable<RexNode> projects = Stream.concat(
          scan.getRowType().getFieldNames().stream().map(relBuilder::field),
          Stream.of(refreshRex(relBuilder)))
          .collect(Collectors.toList());
      relBuilder.project(projects, newFieldNames);

      return relBuilder.build();
    }

    protected abstract RexNode refreshRex(RelBuilder relBuilder);


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
    protected RexNode refreshRex(RelBuilder relBuilder) {
      RelDataType type = relBuilder.field(getRefreshColumn()).getType();
      return relBuilder.getRexBuilder().makeNullLiteral(type);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      RelNode input = aggregate.getInput().accept(this);

      // Create a new project with null UPDATE_COLUMN below aggregate
      final RelBuilder relBuilder = newCalciteRelBuilderWithoutContext(aggregate.getCluster());
      relBuilder.push(input);
      List<RexNode> nodes = input.getRowType().getFieldList().stream().map(q -> {
        if (UPDATE_COLUMN.equals(q.getName())) {
          return relBuilder.getRexBuilder().makeNullLiteral(q.getType());
        } else{
          return relBuilder.getRexBuilder().makeInputRef(q.getType(), q.getIndex());
        }
      }).collect(Collectors.toList());
      relBuilder.project(nodes, input.getRowType().getFieldNames());

      // create a new aggregate with null UPDATE_COLUMN in groupSet
      RelDataType incomingRowType = relBuilder.peek().getRowType();
      RelDataTypeField modField = incomingRowType.getField(UPDATE_COLUMN, false, false);
      ImmutableBitSet newGroupSet = aggregate.getGroupSet().rebuild().set(modField.getIndex()).build();
      GroupKey groupKey = relBuilder.groupKey(newGroupSet, aggregate.indicator, null);

      final int groupCount = aggregate.getGroupCount();
      final Pointer<Integer> ind = new Pointer<>(groupCount-1);
      final List<String> fieldNames = aggregate.getRowType().getFieldNames();
      final List<AggregateCall> aggCalls = aggregate.getAggCallList().stream().map(q -> {
        ind.value++;
        if (q.getName() == null) {
          return q.rename(fieldNames.get(ind.value));
        }
        return q;
      }).collect(Collectors.toList());

      relBuilder.aggregate(groupKey, aggCalls);

      // create a new project on top to preserve rowType
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
    private final UpdateId value;


    public MaterializationShuttle(String refreshColumn, UpdateId value) {
      super(refreshColumn);
      this.value = value;
    }

    private Optional<RexNode> generateLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory, SqlTypeName type) {
      UpdateIdWrapper wrapper = new UpdateIdWrapper(value);
      MinorType minorType = UpdateIdWrapper.getMinorTypeFromSqlTypeName(type);
      wrapper.setType(minorType);
      Object value = wrapper.getObjectValue();
      if (value != null) {
        return Optional.of(rexBuilder.makeLiteral(value, typeFactory.createSqlType(type), false));
      }
      return Optional.empty();
    }

    @Override
    public RelNode visit(TableScan tableScan) {
      if (!(tableScan instanceof IncrementallyUpdateable)) {
        return tableScan;
      }

      final RelNode newScan = updateScan((IncrementallyUpdateable) tableScan);

      // build new filter to apply refresh condition.
      final RexBuilder rexBuilder = tableScan.getCluster().getRexBuilder();
      RelDataTypeField field = newScan.getRowType().getField(UPDATE_COLUMN, false, false);
      final RexNode inputRef = rexBuilder.makeInputRef(newScan, field.getIndex());
      final Optional<RexNode> literal = generateLiteral(rexBuilder, tableScan.getCluster().getTypeFactory(), field.getType().getSqlTypeName());
      if (literal.isPresent()) {
        RexNode condition = tableScan.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.GREATER_THAN, ImmutableList.of(inputRef, literal.get()));
        return LogicalFilter.create(newScan, condition);
      }
      return newScan;
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
    protected RexNode refreshRex(RelBuilder relBuilder) {
      return relBuilder.field(getRefreshColumn());
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
