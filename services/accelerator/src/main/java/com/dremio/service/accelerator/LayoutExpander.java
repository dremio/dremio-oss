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
package com.dremio.service.accelerator;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

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

import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

/**
 * An abstraction used to generate layout plan atop given dataset plan.
 */
public class LayoutExpander {
  private static final Multimap<SqlTypeFamily, SqlAggFunction> AGG_CALLS_PER_TYPE = HashMultimap.create();
  @VisibleForTesting
  static final Multimap<SqlTypeFamily, SqlAggFunction> AGG_CALLS_PER_TYPE_WITH_MIN_MAX = HashMultimap.create();

  private final Multimap<SqlTypeFamily, SqlAggFunction> calls;

  static {
    AGG_CALLS_PER_TYPE_WITH_MIN_MAX.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.SUM0,
        SqlStdOperatorTable.MAX,
        SqlStdOperatorTable.MIN
    ));

    AGG_CALLS_PER_TYPE.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.SUM0
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

  public LayoutExpander(final RelNode view, boolean includeMinMax) {
    this.view = view;
    this.calls = includeMinMax ? AGG_CALLS_PER_TYPE_WITH_MIN_MAX : AGG_CALLS_PER_TYPE;
    this.mappings = FluentIterable
        .from(view.getRowType().getFieldList())
        .uniqueIndex(new Function<RelDataTypeField, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final RelDataTypeField input) {
            return input.getName();
          }
        });
  }

  public RelNode expand(final Layout layout) {
    final LayoutType type = layout.getLayoutType();
    if (type == LayoutType.AGGREGATION) {
      return expandAggregation(layout);
    }

    return expandRaw(layout);
  }

  private RelNode expandRaw(final Layout layout) {
    Preconditions.checkArgument(layout.getLayoutType() == LayoutType.RAW, "required raw layout");

    final List<LayoutField> fields = AccelerationUtils.selfOrEmpty(layout.getDetails().getDisplayFieldList());

    final List<String> names = FluentIterable
        .from(fields)
        .transform(new Function<LayoutField, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final LayoutField input) {
            return input.getName();
          }
        })
        .toList();

    final List<RexInputRef> projections = FluentIterable
        .from(names)
        .transform(new Function<String, RexInputRef>() {
          @Nullable
          @Override
          public RexInputRef apply(@Nullable final String fieldName) {
            final RelDataTypeField field = getField(fieldName);
            return new RexInputRef(field.getIndex(), field.getType());
          }
        })
        .toList();

    return LogicalProject.create(view, projections, names);
  }

  private RelNode expandAggregation(final Layout layout) {
    Preconditions.checkArgument(layout.getLayoutType() == LayoutType.AGGREGATION, "required aggregation layout");

    // create grouping
    final Iterable<Integer> grouping = FluentIterable
        .from(layout.getDetails().getDimensionFieldList())
        .transform(new Function<LayoutDimensionField, Integer>() {
          @Nullable
          @Override
          public Integer apply(@Nullable final LayoutDimensionField dim) {
            return getField(dim.getName()).getIndex();
          }
        });
    final ImmutableBitSet groupSet = ImmutableBitSet.of(grouping);

    // create a project below aggregation
    // use it
    // (i) if we need to a timestamp dimension to date
    // (ii) to project a literal for to be used in sum0(1) for accelerating count(1), sum(1) queries
    final List<RelDataTypeField> fields = view.getRowType().getFieldList();

    final Map<String, LayoutDimensionField> dimensions = FluentIterable.from(layout.getDetails().getDimensionFieldList())
        .uniqueIndex(new Function<LayoutDimensionField, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final LayoutDimensionField input) {
            return input.getName().toLowerCase();
          }
        });

    final RexBuilder rexBuilder = view.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = view.getCluster().getTypeFactory();
    final List<RexNode> projects = FluentIterable
        .from(fields)
        .transform(new Function<RelDataTypeField, RexNode>() {

          @Nullable
          @Override
          public RexNode apply(@Nullable final RelDataTypeField field) {
            final boolean isDimension = groupSet.get(field.getIndex());
            final RexInputRef ref = new RexInputRef(field.getIndex(), field.getType());
            if (!isDimension) {
              return ref;
            }

            final boolean isTimestamp =  field.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP;
            if (!isTimestamp) {
              return ref;
            }

            final LayoutDimensionField dimension = dimensions.get(field.getName().toLowerCase());
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
          @Nullable
          @Override
          public String apply(@Nullable final RelDataTypeField input) {
            return input.getName();
          }
        })
        .append(String.format("one_for_agg_%s", newRandString()))
        .toList();

    final RelNode child = LogicalProject.create(view, projects, fieldNames);
    // create measures
    final List<LayoutField> measures = layout.getDetails().getMeasureFieldList();
    final List<AggregateCall> calls =  FluentIterable.from(AccelerationUtils.selfOrEmpty(measures))
        .transformAndConcat(new Function<LayoutField, Iterable<? extends AggregateCall>>() {
          @Nullable
          @Override
          public Iterable<? extends AggregateCall> apply(@Nullable final LayoutField field) {
            return createMeasuresFor(view, field);
          }
        })
        .append(createDefaultMeasures(child))
        .toList();

    return LogicalAggregate.create(child, false, groupSet, ImmutableList.of(groupSet), calls);
  }

  private Iterable<AggregateCall> createMeasuresFor(final RelNode view, final LayoutField field) {
    final Optional<SqlTypeFamily> family = TypeUtils.getSqlTypeFamily(field);
    if (!family.isPresent()) {
      return ImmutableList.of();
    }

    return FluentIterable
        .from(AccelerationUtils.selfOrEmptyCollection(calls.get(family.get())))
        .transform(new Function<SqlAggFunction, AggregateCall>() {
          private int index = 0;

          @Nullable
          @Override
          public AggregateCall apply(@Nullable final SqlAggFunction func) {
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
            String.format("agg-sum0-%s", newRandString())),
        AggregateCall.create(SqlStdOperatorTable.COUNT, false, ImmutableList.of(literalIndex), -1, 1, view, null,
            String.format("agg-count1-%s", newRandString()))
    );
  }

  private RelDataTypeField getField(final String name) {
    return Preconditions.checkNotNull(mappings.get(name), String.format("unable to find field %s in the view", name));
  }


  private static String newRandString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
