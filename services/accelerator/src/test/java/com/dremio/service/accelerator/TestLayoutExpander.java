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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests layout expander.
 */
public class TestLayoutExpander {

  @Mock
  private RelOptSchema schema;
  @Mock
  private RelOptTable table;

  private RelBuilder builder;
  private RelDataTypeFactory typeFactory;

  /**
   * Interface for verification of an expanded plan.
   */
  public interface Command {
    void validate(RelDataType rowType, RelNode aggregation);
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    final VolcanoPlanner planner = new VolcanoPlanner();
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    builder = DremioRelFactories.LOGICAL_BUILDER.create(cluster, schema);
  }

  @Test
  public void testOneDimensionOneMeasureCombinations() {
    // Set up the dimensions/measures in the test.
    final List<String> names = Arrays.asList("f_dimension", "f_measure_one");
    final List<List<SqlTypeName>> possibleTypes = generateDimensionMeasures();

    // For each possible combination of SQL types in dimensions and measures.
    for (List<SqlTypeName> types : possibleTypes) {
      final RelDataType[] rowTypes = getRowTypes(names, types);

      // Create the layout to be used.
      final int split = 1;
      final Layout layout = createLayout(names, types, split);
      layout.getDetails().getDimensionFieldList().get(0).setGranularity(DimensionGranularity.NORMAL);

      // Test all the possible permutations crossed with nullable combinations.
      testExpandedLayouts(rowTypes, split, layout, new Command() {
        public void validate(RelDataType rowType, RelNode aggregation) {
          compare(rowType, aggregation, names.get(0));
        }
      });
    }
  }

  @Test
  public void testTimestampDimensionWithDateGranularity() {
    // Set up the dimensions/measures in the test.
    final List<String> names = Arrays.asList("f_dimension", "f_measure_one", "f_measure_two");
    final List<SqlTypeName> types = Arrays.asList(SqlTypeName.TIMESTAMP, SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
    final RelDataType[] rowTypes = getRowTypes(names, types);

    // Create the layout to be used.
    final int split = 1;
    final Layout layout = createLayout(names, types, split);
    layout.getDetails().getDimensionFieldList().get(0).setGranularity(DimensionGranularity.DATE);

    // Test all the possible permutations crossed with nullable combinations.
    testExpandedLayouts(rowTypes, split, layout, new Command() {
      public void validate(RelDataType rowType, RelNode aggregation) {
        compareWithType(rowType, aggregation, names.get(0), SqlTypeName.DATE);
      }
    });
  }

  @Test
  public void testTimestampDimensionWithNormalGranularity() {
    // Set up the dimensions/measures in the test.
    final List<String> names = Arrays.asList("f_dimension", "f_measure_one", "f_measure_two");
    final List<SqlTypeName> types = Arrays.asList(SqlTypeName.TIMESTAMP, SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
    final RelDataType[] rowTypes = getRowTypes(names, types);

    // Create the layout to be used.
    final int split = 1;
    final Layout layout = createLayout(names, types, split);
    layout.getDetails().getDimensionFieldList().get(0).setGranularity(DimensionGranularity.NORMAL);

    // Test all the possible permutations crossed with nullable combinations.
    testExpandedLayouts(rowTypes, split, layout, new Command() {
      public void validate(RelDataType rowType, RelNode aggregation) {
        compare(rowType, aggregation, names.get(0));
      }
    });
  }

  @Test
  public void testMultiTimestampDimensionCastedToDate() {
    // Set up the dimensions/measures in the test.
    final List<String> names = Arrays.asList("f_dimension_one", "f_dimension_two", "f_measure_two");
    final List<SqlTypeName> types = Arrays.asList(SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP, SqlTypeName.DOUBLE);
    final RelDataType[] rowTypes = getRowTypes(names, types);

    // Create the layout to be used.
    final int split = 2;
    final Layout layout = createLayout(names, types, split);
    layout.getDetails().getDimensionFieldList().get(0).setGranularity(DimensionGranularity.DATE);
    layout.getDetails().getDimensionFieldList().get(1).setGranularity(DimensionGranularity.NORMAL);

    // Test all the possible permutations crossed with nullable combinations.
    testExpandedLayouts(rowTypes, split, layout, new Command() {
      public void validate(RelDataType rowType, RelNode aggregation) {
        compareWithType(rowType, aggregation, names.get(0), SqlTypeName.DATE);
        compare(rowType, aggregation, names.get(1));
      }
    });
  }

  @Test
  public void testAggregationExpansionAddsDefaultMeasures() {
    // Set up the dimensions/measures in the test.
    final List<String> names = Arrays.asList("f_dimension_one", "f_measure_one");
    final List<SqlTypeName> types = Arrays.asList(SqlTypeName.TIMESTAMP, SqlTypeName.DOUBLE);
    final RelDataType[] rowTypes = getRowTypes(names, types);

    // Create the layout to be used.
    final int split = 1;
    final Layout layout = createLayout(names, types, split);

    // Test all the possible permutations crossed with nullable combinations.
    testExpandedLayouts(rowTypes, split, layout, new Command() {
      public void validate(RelDataType rowType, RelNode aggregation) {
        compareWithType(rowType, aggregation, names.get(0), SqlTypeName.DATE);

        final Aggregate aggregate = (Aggregate)aggregation;
        final Project child = (Project) aggregate.getInput();

        final List<AggregateCall> calls = aggregate.getAggCallList();
        assertTrue(calls.size() > 2);
        // check sum0(1)
        final AggregateCall sumCall = calls.get(calls.size() - 2);
        assertEquals(SqlKind.SUM0, sumCall.getAggregation().getKind());
        assertEquals(1, sumCall.getArgList().size());
        final int sumRefIndex = sumCall.getArgList().get(0);
        assertEquals(SqlKind.LITERAL, child.getProjects().get(sumRefIndex).getKind());

        // check count(1)
        final AggregateCall countCall = calls.get(calls.size() - 1);
        assertEquals(SqlKind.COUNT, countCall.getAggregation().getKind());
        assertEquals(1, countCall.getArgList().size());
        final int countRefIndex = sumCall.getArgList().get(0);
        assertEquals(SqlKind.LITERAL, child.getProjects().get(countRefIndex).getKind());
      }
    });
  }

  private void testExpandedLayouts(RelDataType[] rowTypes, int split, Layout layout, Command command) {
    for (final RelDataType rowType : rowTypes) {
      when(schema.getTableForMember(anyListOf(String.class))).thenReturn(table);
      when(table.getRowType()).thenReturn(rowType);

      final RelNode rel = builder.scan("xyz").build();
      final LayoutExpander expander = new LayoutExpander(rel, true);
      final RelNode aggregation = expander.expand(layout);

      assertTrue(Aggregate.class.isAssignableFrom(aggregation.getClass()));

      // 2 default aggregations + number of dimensions
      int numFields = 2 + split;
      Collection<SqlAggFunction> emptyList = ImmutableList.of();

      // Determine number of measures based on measure type.
      for (LayoutField field :layout.getDetails().getMeasureFieldList()) {
        numFields += Optional
            .fromNullable(LayoutExpander.AGG_CALLS_PER_TYPE_WITH_MIN_MAX.get(TypeUtils.getSqlTypeFamily(field).get()))
            .or(emptyList)
            .size();
       }

      // 2 default aggregations + number dimensions + (4 * number measures)
      assertEquals(numFields, aggregation.getRowType().getFieldCount());

      command.validate(rowType, aggregation);
    }
  }

  private void compare(RelDataType rowType, RelNode aggregation, String fieldName) {
    assertEquals(
      rowType.getField(fieldName, false, false).getType(),
      aggregation.getRowType().getField(fieldName, false, false).getType());
  }

  private void compareWithType(RelDataType rowType, RelNode aggregation, String fieldName, SqlTypeName typeName) {
    RelDataType rowField = rowType.getField(fieldName, false, false).getType();
    RelDataType aggField = aggregation.getRowType().getField(fieldName, false, false).getType();
    assertEquals(rowField.isNullable(), aggField.isNullable());
    assertEquals(rowField.getPrecision(), aggField.getPrecision());
    assertEquals(rowField.getScale(), aggField.getScale());
    assertEquals(typeName, aggField.getSqlTypeName());
  }

  private Layout createLayout(List<String> names, List<SqlTypeName> types, int split) {
    List<LayoutDimensionField> dimensionFields = new ArrayList<>();
    List<LayoutField> measureFields = new ArrayList<>();
    for (int i = 0; i < names.size(); ++i) {
      if (i < split) {
        dimensionFields.add(
          new LayoutDimensionField()
            .setName(names.get(i))
            .setTypeFamily(types.get(i).getFamily().toString()));
      } else {
        measureFields.add(
          new LayoutField()
            .setName(names.get(i))
            .setTypeFamily(types.get(i).getFamily().toString()));
      }
    }

    return new Layout()
      .setLayoutType(LayoutType.AGGREGATION)
      .setDetails(
        new LayoutDetails()
          .setDimensionFieldList(dimensionFields)
          .setMeasureFieldList(measureFields));
  }

  private RelDataType[] getRowTypes(List<String> names, List<SqlTypeName> types) {
    // Generate all the combinations of nullable types.
    List<List<Boolean>> nullablePerms = generateNullableCombinations(names.size());

    // Generate the permutations of names and types.
    List<List<String>> namePerms = new ArrayList<>();
    permutations(names, namePerms, 0);
    List<List<SqlTypeName>> typePerms = new ArrayList<>();
    permutations(types, typePerms, 0);

    // Generate all possible combinations of the columns and nullable.
    List<RelDataType> relDataTypes = new ArrayList<>();
    for (List<Boolean> nullable : nullablePerms) {
      for (int i = 0; i < namePerms.size(); ++i) {
        List<String> curNames = namePerms.get(i);
        List<SqlTypeName> curTypes = typePerms.get(i);

        List<RelDataType> curRelDataTypes = new ArrayList<>();
        for (int j = 0; j < curTypes.size(); ++j) {
          if (nullable.get(j)) {
            curRelDataTypes.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(curTypes.get(j)), true));
          } else {
            curRelDataTypes.add(typeFactory.createSqlType(curTypes.get(j)));
          }
        }

        relDataTypes.add(typeFactory.createStructType(curRelDataTypes, curNames));
      }
    }

    return relDataTypes.toArray(new RelDataType[0]);
  }

  private List<List<SqlTypeName>> generateDimensionMeasures() {
    // Generates combinations of one measure/dimension across common types.
    List<List<SqlTypeName>> combos = new ArrayList<>();

    for (SqlTypeName dimType : SqlTypeName.values()) {
      if (SqlTypeName.INTERVAL_TYPES.contains(dimType) ||
          (SqlTypeName.MULTISET == dimType) ||
          (null == dimType.getFamily())) {
        continue;
      }

      for (SqlTypeName measureType : SqlTypeName.values()) {
        if (!SqlTypeName.BOOLEAN_TYPES.contains(measureType) &&
            !SqlTypeName.STRING_TYPES.contains(measureType) &&
            !SqlTypeName.NUMERIC_TYPES.contains(measureType) &&
            !SqlTypeName.DATETIME_TYPES.contains(measureType)) {
          continue;
        }

        List<SqlTypeName> dimMeasure = new ArrayList<>();
        dimMeasure.add(dimType);
        dimMeasure.add(measureType);
        combos.add(dimMeasure);
      }
    }

    return combos;
  }

  private List<List<Boolean>> generateNullableCombinations(int size) {
    Set<List<Boolean>> nullableCombos = new HashSet<>();
    List<List<Boolean>> tempPerms = new ArrayList<>();

    // Set up a boolean for each field.
    List<Boolean> bools = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      bools.add(Boolean.FALSE);
    }

    // Walk through the booleans, flipping them one at a time, and generate the possible permutations
    // of that combination of booleans, and record them.
    for (int i = -1; i < size; ++i) {
      if (i >= 0) {
        bools.set(i, Boolean.TRUE);
      }

      permutations(bools, tempPerms, 0);
      nullableCombos.addAll(tempPerms);
    }

    return new ArrayList<>(nullableCombos);
  }

  private static <E> void permutations(List<E> list, List<List<E>> generatedPermutations, int index) {
    if (index >= (list.size() - 1)) {
      generatedPermutations.add(ImmutableList.copyOf(list));
      return;
    }

    for (int i = index; i < list.size(); ++i) {
      Collections.swap(list, i, index);
      permutations(list, generatedPermutations, index + 1);
      Collections.swap(list, index, i);
    }
  }
}

