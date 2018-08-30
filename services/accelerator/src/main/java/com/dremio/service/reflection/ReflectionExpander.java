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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.expr.fn.hll.HyperLogLog;
import com.dremio.exec.store.Views;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.MeasureType;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * An abstraction used to generate reflection plan atop given dataset plan.
 */
public class ReflectionExpander {

  private static final ImmutableList<MeasureType> DEFAULT_MEASURE_LIST = ImmutableList.of(MeasureType.COUNT, MeasureType.MAX, MeasureType.MIN, MeasureType.SUM);

  private final RelNode view;
  private final Map<String, RelDataTypeField> mappings;
  private final Map<String, ViewFieldType> fields;

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

  public ReflectionExpander(final RelNode view, DatasetConfig dataset) {
    Map<String, ViewFieldType> fields = computeFieldTypes(dataset, view);
    this.view = view;
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

    final List<String> names = fields.stream().map(field -> field.getName()).collect(Collectors.toList());

    final List<RexInputRef> projections = names.stream()
        .map(fieldName -> {
            final RelDataTypeField field = getField(fieldName);
            return new RexInputRef(field.getIndex(), field.getType());
          })
        .collect(Collectors.toList());

    return LogicalProject.create(view, projections, names);
  }

  private RelNode expandAggregation(final ReflectionGoal goal) {
    Preconditions.checkArgument(goal.getType() == AGGREGATION, "required aggregation reflection");

    // create grouping
    List<ReflectionDimensionField> dimensionFieldList = AccelerationUtils.selfOrEmpty(goal.getDetails().getDimensionFieldList());

    final Iterable<Integer> grouping = dimensionFieldList.stream()
        .map(dim -> getField(dim.getName()).getIndex())
        .collect(Collectors.toList());
    final ImmutableBitSet groupSet = ImmutableBitSet.of(grouping);

    // create a project below aggregation
    // use it
    // (i) if we need to a timestamp dimension to date
    // (ii) to project a literal for to be used in sum0(1) for accelerating count(1), sum(1) queries
    final List<RelDataTypeField> fields = view.getRowType().getFieldList();

    final Map<String, ReflectionDimensionField> dimensions = FluentIterable.from(dimensionFieldList)
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
    final List<ReflectionMeasureField> measures = goal.getDetails().getMeasureFieldList();
    final List<AggregateCall> calls =  Stream.concat(
          AccelerationUtils.selfOrEmpty(measures)
          .stream()
          .flatMap(this::toCalls)
          ,
          createDefaultMeasures(child))
        .collect(Collectors.toList());

    return LogicalAggregate.create(child, false, groupSet, ImmutableList.of(groupSet), calls);
  }

  private Stream<AggregateCall> toCalls(ReflectionMeasureField field) {
    Optional<SqlTypeFamily> typeFamily = getSqlTypeFamily(field.getName());
    if(!typeFamily.isPresent()) {
      // are we silently not measuring the field ? should we catch this during validation ?
      return Stream.of();
    }

    // for old systems, make sure we have a default measure list if one is not specificed.
    List<MeasureType> measures = field.getMeasureTypeList() == null || field.getMeasureTypeList().isEmpty() ? DEFAULT_MEASURE_LIST : field.getMeasureTypeList();
    List<AggregateCall> calls = new ArrayList<>();
    final int inputRef = getField(field.getName()).getIndex();
    int inFieldIndex = 0;
    for(MeasureType t : measures) {
      AggregateCall c = createMeasureFor(inputRef, inFieldIndex, typeFamily.get(), t);
      if(c == null) {
        continue;
      }
      calls.add(c);
    }
    return calls.stream();
  }

  /**
   * Get the type family of the requested field.
   * @param name The name of the field.
   * @return The type family or Option.absent() if no family was found/determined.
   */
  private Optional<SqlTypeFamily> getSqlTypeFamily(final String name) {
    if (fields.containsKey(name)) {
      final ViewFieldType field = fields.get(name);
      try {
        return Optional.of(SqlTypeFamily.valueOf(field.getTypeFamily()));
      } catch (final IllegalArgumentException ex) {
        // return absent
      }
    }
      return Optional.absent();
  }

  /**
   * For a particular input and type family, create the request type if it is allowed.
   * @param inputRef The input of that this measure will be applied to.
   * @param index The index of this measure when the collection of measure for this input field.
   * @param family The type family of the field.
   * @param type The type of measure to generate.
   * @return An aggregate call or null if we can't create a measure of the requested type.
   */
  private AggregateCall createMeasureFor(int inputRef, int index, SqlTypeFamily family, MeasureType type) {

    // skip measure columns for invalid types.
    if(!ReflectionValidator.getValidMeasures(family).contains(type)) {
      return null;
    }

    switch(type) {
    case APPROX_COUNT_DISTINCT:
      return AggregateCall.create(HyperLogLog.HLL, false, ImmutableList.of(inputRef), -1, 1, view, null, String.format("agg-%s-%s", inputRef, index));
    case COUNT:
      return AggregateCall.create(SqlStdOperatorTable.COUNT, false, ImmutableList.of(inputRef), -1, 1, view, null, String.format("agg-%s-%s", inputRef, index));
    case MAX:
      return AggregateCall.create(SqlStdOperatorTable.MAX, false, ImmutableList.of(inputRef), -1, 1, view, null, String.format("agg-%s-%s", inputRef, index));
    case MIN:
      return AggregateCall.create(SqlStdOperatorTable.MIN, false, ImmutableList.of(inputRef), -1, 1, view, null, String.format("agg-%s-%s", inputRef, index));
    case SUM:
      return AggregateCall.create(SqlStdOperatorTable.SUM, false, ImmutableList.of(inputRef), -1, 1, view, null, String.format("agg-%s-%s", inputRef, index));
    case UNKNOWN:
    default:
      throw new UnsupportedOperationException(type.name());
    }
  }

  private Stream<AggregateCall> createDefaultMeasures(final RelNode view) {
    final int literalIndex = view.getRowType().getFieldCount() -1;
    return Stream.of(
        AggregateCall.create(SqlStdOperatorTable.SUM0, false, ImmutableList.of(literalIndex), -1, 1, view, null, "agg-sum0-0"),
        AggregateCall.create(SqlStdOperatorTable.COUNT, false, ImmutableList.of(literalIndex), -1, 1, view, null, "agg-count1-0")
        );
  }

  private RelDataTypeField getField(final String name) {
    return Preconditions.checkNotNull(mappings.get(name), String.format("unable to find field %s in the view", name));
  }

}
