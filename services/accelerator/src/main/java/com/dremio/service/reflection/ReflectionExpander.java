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
package com.dremio.service.reflection;

import static com.dremio.service.reflection.ReflectionUtils.removeUpdateColumn;
import static com.dremio.service.reflection.proto.ReflectionType.AGGREGATION;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.store.Views;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * An abstraction used to generate reflection plan atop given dataset plan.
 */
public class ReflectionExpander {
  private static final Multimap<SqlTypeFamily, SqlAggFunction> AGG_CALLS_PER_TYPE = HashMultimap.create();
  private static final Multimap<SqlTypeFamily, SqlAggFunction> AGG_CALLS_PER_TYPE_WITH_MIN_MAX = HashMultimap.create();

  private final Multimap<SqlTypeFamily, SqlAggFunction> calls;
  private final Map<String, ViewFieldType> fields;

  static {
    AGG_CALLS_PER_TYPE_WITH_MIN_MAX.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.SUM,
        SqlStdOperatorTable.MAX,
        SqlStdOperatorTable.MIN
    ));

    AGG_CALLS_PER_TYPE.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.SUM
    ));

    for (SqlTypeFamily type: ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.DATE, SqlTypeFamily.TIME)) {
      AGG_CALLS_PER_TYPE_WITH_MIN_MAX.putAll(type, ImmutableList.of(
          SqlStdOperatorTable.COUNT,
          SqlStdOperatorTable.MAX,
          SqlStdOperatorTable.MIN
      ));
      AGG_CALLS_PER_TYPE.putAll(SqlTypeFamily.CHARACTER, ImmutableList.of(
          SqlStdOperatorTable.COUNT
      ));
    }
  }

  private final RelNode view;

  private final Map<String, RelDataTypeField> mappings;



  private static Map<String, ViewFieldType> computeFieldTypes(final DatasetConfig dataset, final RelNode plan) {
    // based off AnalysisState
    List<ViewFieldType> fields = ViewFieldsHelper.getViewFields(dataset);
    if (fields == null || fields.isEmpty()) {
      fields = Views.viewToFieldTypes(Views.relDataTypeToFieldType(plan.getRowType()));
    }
    fields = removeUpdateColumn(fields);

    Preconditions.checkState(plan.getRowType().getFieldCount() == fields.size(),
      String.format("Found mismatching number of FieldTypes (%d) to RelDataTypes (%d):  %s, %s",
        fields.size(), plan.getRowType().getFieldCount(), fields, plan.getRowType().toString()));
    return Maps.uniqueIndex(fields, new Function<ViewFieldType, String>() {
      @Override
      public String apply(ViewFieldType field) {
        return field.getName();
      }
    });
  }

  public ReflectionExpander(final RelNode view, DatasetConfig dataset, boolean includeMinMax) {
    Map<String, ViewFieldType> fields = computeFieldTypes(dataset, view);
    this.view = view;
    this.calls = includeMinMax ? AGG_CALLS_PER_TYPE_WITH_MIN_MAX : AGG_CALLS_PER_TYPE;
    this.fields = Preconditions.checkNotNull(fields, "fields is required");
    this.mappings = FluentIterable
      .from(view.getRowType().getFieldList())
      .uniqueIndex(new Function<RelDataTypeField, String>() {
        @Override
        public String apply(final RelDataTypeField field) {
          return field.getName();
        }
      });
  }

  public RelNode expand(final ReflectionGoal goal) {
    final ReflectionType type = goal.getType();
    if (type == AGGREGATION) {
      return expandAggregation(goal);
    }

    return expandRaw(goal);
  }

  private RelNode expandRaw(final ReflectionGoal goal) {
    Preconditions.checkArgument(goal.getType() == ReflectionType.RAW, "required raw reflection");

    final List<ReflectionField> fields = AccelerationUtils.selfOrEmpty(goal.getDetails().getDisplayFieldList());

    final List<String> names = FluentIterable
        .from(fields)
        .transform(new Function<ReflectionField, String>() {
          @Override
          public String apply(final ReflectionField field) {
            return field.getName();
          }
        })
        .toList();

    final List<RexInputRef> projections = FluentIterable
        .from(names)
        .transform(new Function<String, RexInputRef>() {
          @Override
          public RexInputRef apply(final String fieldName) {
            final RelDataTypeField field = getField(fieldName);
            return new RexInputRef(field.getIndex(), field.getType());
          }
        })
        .toList();

    return LogicalProject.create(view, projections, names);
  }

  private RelNode expandAggregation(final ReflectionGoal goal) {
    Preconditions.checkArgument(goal.getType() == AGGREGATION, "required aggregation reflection");

    // create grouping
    final Iterable<Integer> grouping = FluentIterable
        .from(goal.getDetails().getDimensionFieldList())
        .transform(new Function<ReflectionDimensionField, Integer>() {
          @Override
          public Integer apply(final ReflectionDimensionField dim) {
            return getField(dim.getName()).getIndex();
          }
        });
    final ImmutableBitSet groupSet = ImmutableBitSet.of(grouping);

    // create a project below aggregation
    // use it
    // (i) if we need to a timestamp dimension to date
    // (ii) to project a literal for to be used in sum0(1) for accelerating count(1), sum(1) queries
    final List<RelDataTypeField> fields = view.getRowType().getFieldList();

    final Map<String, ReflectionDimensionField> dimensions = FluentIterable.from(goal.getDetails().getDimensionFieldList())
        .uniqueIndex(new Function<ReflectionDimensionField, String>() {
          @Override
          public String apply(final ReflectionDimensionField field) {
            return field.getName().toLowerCase();
          }
        });

    final RexBuilder rexBuilder = view.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = view.getCluster().getTypeFactory();
    final List<RexNode> projects = FluentIterable
        .from(fields)
        .transform(new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(final RelDataTypeField field) {
            final boolean isDimension = groupSet.get(field.getIndex());
            final RexInputRef ref = new RexInputRef(field.getIndex(), field.getType());
            if (!isDimension) {
              return ref;
            }

            final boolean isTimestamp =  field.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP;
            if (!isTimestamp) {
              return ref;
            }

            final ReflectionDimensionField dimension = dimensions.get(field.getName().toLowerCase());
            final DimensionGranularity granularity = Optional.fromNullable(dimension.getGranularity()).or(DimensionGranularity.DATE);
            switch (granularity) {
              case NORMAL:
                return ref;
              case DATE:
                // DX-4754:  must match nullability for these fields, otherwise the materialized view has incorrect row type/field types.
                // The underlying view can have nullability set to true, but materialization layout would say otherwise and lead to incorrect behavior.
                return rexBuilder.makeCast(
                    typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), field.getType().isNullable()),
                    ref);
              default:
                throw new UnsupportedOperationException(String.format("unsupported dimension granularity: %s", granularity));
            }

          }
        })
        .append(rexBuilder.makeBigintLiteral(BigDecimal.ONE))
        .toList();

    final List<String> fieldNames = FluentIterable
        .from(fields)
        .transform(new Function<RelDataTypeField, String>() {
          @Override
          public String apply(final RelDataTypeField field) {
            return field.getName();
          }
        })
        .append("one_for_agg_0")
        .toList();

    final RelNode child = LogicalProject.create(view, projects, fieldNames);
    // create measures
    final List<ReflectionField> measures = goal.getDetails().getMeasureFieldList();
    final List<AggregateCall> calls =  FluentIterable.from(AccelerationUtils.selfOrEmpty(measures))
        .transformAndConcat(new Function<ReflectionField, Iterable<? extends AggregateCall>>() {
          @Override
          public Iterable<? extends AggregateCall> apply(final ReflectionField field) {
            return createMeasuresFor(view, field);
          }
        })
        .append(createDefaultMeasures(child))
        .toList();

    return LogicalAggregate.create(child, false, groupSet, ImmutableList.of(groupSet), calls);
  }

  private Optional<SqlTypeFamily> getSqlTypeFamily(final ReflectionField fieldDescriptor) {
    if (fields.containsKey(fieldDescriptor.getName())) {
      final ViewFieldType field = fields.get(fieldDescriptor.getName());
      try {
        return Optional.of(SqlTypeFamily.valueOf(field.getTypeFamily()));
      } catch (final IllegalArgumentException ex) {
        // return absent
      }
    }
      return Optional.absent();
  }

  private Iterable<AggregateCall> createMeasuresFor(final RelNode view, final ReflectionField field) {
    final Optional<SqlTypeFamily> family = getSqlTypeFamily(field);
    if (!family.isPresent()) {
      // are we silently not measuring the field ? should we catch this during validation ?
      return ImmutableList.of();
    }

    return FluentIterable
      .from(AccelerationUtils.selfOrEmptyCollection(calls.get(family.get())))
      .transform(new Function<SqlAggFunction, AggregateCall>() {
        private int index = 0;

        @Override
        public AggregateCall apply(final SqlAggFunction func) {
          // no distinct measures for now
          final int inputRef = getField(field.getName()).getIndex();
          return AggregateCall.create(func, false, ImmutableList.of(inputRef), -1, 1, view, null,
            String.format("agg-%s-%s", inputRef, index++));
        }
      });
  }

  private Iterable<AggregateCall> createDefaultMeasures(final RelNode view) {
    final int literalIndex = view.getRowType().getFieldCount() -1;
    return ImmutableList.of(
        AggregateCall.create(SqlStdOperatorTable.SUM0, false, ImmutableList.of(literalIndex), -1, 1, view, null,
            "agg-sum0-0"),
        AggregateCall.create(SqlStdOperatorTable.COUNT, false, ImmutableList.of(literalIndex), -1, 1, view, null,
            "agg-count1-0")
    );
  }

  private RelDataTypeField getField(final String name) {
    return Preconditions.checkNotNull(mappings.get(name), String.format("unable to find field %s in the view", name));
  }


//  private static String newRandString() {
//    return Long.toHexString(Double.doubleToLongBits(Math.random()));
//  }
}
