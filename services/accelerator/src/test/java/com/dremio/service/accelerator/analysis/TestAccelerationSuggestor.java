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
package com.dremio.service.accelerator.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.ColumnStats;
import com.dremio.service.accelerator.proto.DatasetAnalysis;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.collect.ImmutableList;

/**
 * "True" unit tests for {@link AccelerationSuggestor}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestAccelerationSuggestor {

  private static final int MAX_DIMENSION_FIELDS = 30;
  private static final int MAX_DIMENSION_FIELDS_TEMP = 3;

  private  ColumnStats create(String name, long cardinality) {
    return create(name, cardinality, SqlTypeName.VARCHAR);
  }

  private  ColumnStats create(String name, long cardinality, SqlTypeName type) {
    return new ColumnStats()
        .setField(new LayoutField()
            .setName(name)
            .setTypeFamily(type.getFamily().toString()))
        .setCardinality(cardinality);
  }

  @Test
  public void testRawSuggestionWithNonNumericSchema() {
    // setup
    final Acceleration acceleration = new Acceleration();

    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final String fieldName = "some-field";
    final ViewFieldType field = new ViewFieldType().setName(fieldName).setTypeFamily(typeFamily).setType(type);

    acceleration.setContext(
        new AccelerationContext()
            .setDatasetSchema(
                new RowType().setFieldList(ImmutableList.of(field))
            )
    );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeRawLayouts(acceleration);


    // verify raw acceleration
    assertEquals(1, acceleration.getRawLayouts().getLayoutList().size());

    final LayoutContainer rawLayouts = acceleration.getRawLayouts();
    assertEquals(LayoutType.RAW, rawLayouts.getType());
    assertFalse(rawLayouts.getEnabled());

    final LayoutDetails details = acceleration.getRawLayouts().getLayoutList().get(0).getDetails();
    assertEquals(1, details.getDisplayFieldList().size());
    assertEquals(fieldName, details.getDisplayFieldList().get(0).getName());
    assertEquals(typeFamily, details.getDisplayFieldList().get(0).getTypeFamily());

    assertTrue(AccelerationUtils.selfOrEmpty(details.getDimensionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getMeasureFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getPartitionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getSortFieldList()).isEmpty());
  }

  @Test
  public void testAggregationSuggestionWithNonNumericSchema() {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final String fieldName = "some-field";
    final ViewFieldType field = new ViewFieldType().setName(fieldName).setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(new ColumnStats()
                    .setField(new LayoutField()
                        .setName(fieldName)
                        .setTypeFamily(typeFamily)
                    ))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field)))
        );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(1, details.getDimensionFieldList().size());
    assertEquals(fieldName, details.getDimensionFieldList().get(0).getName());
    assertEquals(typeFamily, details.getDimensionFieldList().get(0).getTypeFamily());

    assertTrue(AccelerationUtils.selfOrEmpty(details.getDisplayFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getMeasureFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getPartitionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getSortFieldList()).isEmpty());
  }

  /*
   * Suggest only top 30 dimensions whose cardinality sum is <= square root of sum of all cardinalities
   */
  @Test
  public void testSingleLayoutSuggestionWithLowNumbers() {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 9800L),
                    create("field1", 10L),
                    create("field2", 90L),
                    create("field3", 100L))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3)))
        );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(2, details.getDimensionFieldList().size());

    assertEquals("field1", details.getDimensionFieldList().get(0).getName());
    assertEquals("field2", details.getDimensionFieldList().get(1).getName());

    assertTrue(AccelerationUtils.selfOrEmpty(details.getDisplayFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getMeasureFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getPartitionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getSortFieldList()).isEmpty());
  }

  /*
   * Suggest only top 30 dimensions whose cardinality sum is <= square root of sum of all cardinalities
   *
   */
  @Test
  public void testSingleLayoutSuggestionWithHigherNumbers() {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field6 = new ViewFieldType().setName("field6").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field7 = new ViewFieldType().setName("field7").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field8 = new ViewFieldType().setName("field8").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 1000000L),
                    create("field1", 10L),
                    create("field2", 100L),
                    create("field3", 100L),
                    create("field4", 700L),
                    create("field5", 1000L),
                    create("field6", 1000L),
                    create("field7", 1000L),
                    create("field8", 1000L))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5, field6, field7, field8)))
        );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field1", details.getDimensionFieldList().get(0).getName());
    assertEquals("field2", details.getDimensionFieldList().get(1).getName());
    assertEquals("field3", details.getDimensionFieldList().get(2).getName());
    assertEquals("field4", details.getDimensionFieldList().get(3).getName());

    assertTrue(AccelerationUtils.selfOrEmpty(details.getDisplayFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getMeasureFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getPartitionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getSortFieldList()).isEmpty());
  }

  /*
   * Suggest only top 30 dimensions whose cardinality sum is <= square root of sum of all cardinalities
   * and cartesian product <= 2 Billion (assuming 500 bytes per row, its 1 TB just based on sampled dataset)
   *
   */
  @Test
  public void testHigherCardinalityProduct() {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 10L),
                    create("field1", 20L),
                    create("field2", 1000L),
                    create("field3", 10000L),
                    create("field4", 10000L),
                    create("field5", 1000000000L))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5)))
        );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertTrue(AccelerationUtils.selfOrEmpty(details.getDisplayFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getMeasureFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getPartitionFieldList()).isEmpty());
    assertTrue(AccelerationUtils.selfOrEmpty(details.getSortFieldList()).isEmpty());
  }

  @Test
  public void testExclusiveMeasuresAndDimensions () {
    // setup
    final String typeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String type = SqlTypeName.INTEGER.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 10L, SqlTypeName.INTEGER),
                    create("field1", 20L, SqlTypeName.INTEGER),
                    create("field2", 1000L, SqlTypeName.INTEGER),
                    create("field3", 10000L, SqlTypeName.INTEGER),
                    create("field4", 10000L, SqlTypeName.INTEGER),
                    create("field5", 1000000000L, SqlTypeName.INTEGER))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5)))
        );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(2, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());
    assertEquals("field4", details.getMeasureFieldList().get(1).getName());
  }

  @Test
  public void testMeasuresWithNonNumericSchema() {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 10L),
                    create("field1", 20L),
                    create("field2", 1000L),
                    create("field3", 10000L),
                    create("field4", 10000L),
                    create("field5", 1000000000L))
                )
            ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5)))
        );
    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(0, details.getMeasureFieldList().size());
  }

  @Test
  public void testExclusiveMeasuresAndDimensionsWithLimit () {
    // setup
    final String typeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String type = SqlTypeName.INTEGER.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type);

    final Acceleration acceleration = new Acceleration()
      .setContext(new AccelerationContext()
        .setAnalysis(new DatasetAnalysis()
          .setColumnList(ImmutableList.of(
            create("field0", 10L, SqlTypeName.INTEGER),
            create("field1", 20L, SqlTypeName.INTEGER),
            create("field2", 1000L, SqlTypeName.INTEGER),
            create("field3", 10000L, SqlTypeName.INTEGER),
            create("field4", 10000L, SqlTypeName.INTEGER),
            create("field5", 1000000000L, SqlTypeName.INTEGER))
          )
        ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5)))
      );

    /*
     * DX-7864: test to limit the max number of dimensions for reflections to 3
     */
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS_TEMP);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(3, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());

    assertEquals(3, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());
    assertEquals("field4", details.getMeasureFieldList().get(1).getName());
    assertEquals("field3", details.getMeasureFieldList().get(2).getName());
  }

  @Test
  public void testExclusiveMeasuresAndDimensionsWithDifferentTypes () {
    // setup
    final String intTypeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String floatTypeFamily = SqlTypeName.FLOAT.getFamily().toString();
    final String intType = SqlTypeName.INTEGER.getName();
    final String floatType = SqlTypeName.FLOAT.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field6 = new ViewFieldType().setName("field6").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field7 = new ViewFieldType().setName("field7").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field8 = new ViewFieldType().setName("field8").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field9 = new ViewFieldType().setName("field9").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field10 = new ViewFieldType().setName("field10").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field11= new ViewFieldType().setName("field11").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field12= new ViewFieldType().setName("field12").setTypeFamily(floatTypeFamily).setType(floatType);

    final Acceleration acceleration = new Acceleration()
      .setContext(new AccelerationContext()
        .setAnalysis(new DatasetAnalysis()
          .setColumnList(ImmutableList.of(
            create("field0", 10L, SqlTypeName.INTEGER),
            create("field1", 10L, SqlTypeName.FLOAT),
            create("field2", 10L, SqlTypeName.INTEGER),
            create("field3", 10L, SqlTypeName.FLOAT),
            create("field4", 10L, SqlTypeName.INTEGER),
            create("field5", 10L, SqlTypeName.FLOAT),
            create("field6", 10L, SqlTypeName.INTEGER),
            create("field7", 10L, SqlTypeName.FLOAT),
            create("field8", 10L, SqlTypeName.INTEGER),
            create("field9", 10L, SqlTypeName.FLOAT),
            create("field10", 10L, SqlTypeName.INTEGER),
            create("field11", 1000000L, SqlTypeName.INTEGER),
            create("field12", 1000000L, SqlTypeName.FLOAT))
          )
        ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12)))
      );

    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();

    assertEquals(6, details.getDimensionFieldList().size());
    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field2", details.getDimensionFieldList().get(1).getName());
    assertEquals("field4", details.getDimensionFieldList().get(2).getName());
    assertEquals("field6", details.getDimensionFieldList().get(3).getName());
    assertEquals("field8", details.getDimensionFieldList().get(4).getName());
    assertEquals("field10", details.getDimensionFieldList().get(5).getName());

    assertEquals(6, details.getMeasureFieldList().size());
    assertEquals("field12", details.getMeasureFieldList().get(0).getName());
    assertEquals("field11", details.getMeasureFieldList().get(1).getName());
    assertEquals("field9", details.getMeasureFieldList().get(2).getName());
    assertEquals("field7", details.getMeasureFieldList().get(3).getName());
    assertEquals("field5", details.getMeasureFieldList().get(4).getName());
    assertEquals("field3", details.getMeasureFieldList().get(5).getName());
  }

  @Test
  public void testExclusiveMeasuresAndDimensionsWithDifferentTypesAndLimit () {
    // setup
    final String intTypeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String floatTypeFamily = SqlTypeName.FLOAT.getFamily().toString();
    final String intType = SqlTypeName.INTEGER.getName();
    final String floatType = SqlTypeName.FLOAT.getName();
    final ViewFieldType field0 = new ViewFieldType().setName("field0").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field1 = new ViewFieldType().setName("field1").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field2 = new ViewFieldType().setName("field2").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field3 = new ViewFieldType().setName("field3").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field4 = new ViewFieldType().setName("field4").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field5 = new ViewFieldType().setName("field5").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field6 = new ViewFieldType().setName("field6").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field7 = new ViewFieldType().setName("field7").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field8 = new ViewFieldType().setName("field8").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field9 = new ViewFieldType().setName("field9").setTypeFamily(floatTypeFamily).setType(floatType);
    final ViewFieldType field10 = new ViewFieldType().setName("field10").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field11= new ViewFieldType().setName("field11").setTypeFamily(intTypeFamily).setType(intType);
    final ViewFieldType field12= new ViewFieldType().setName("field12").setTypeFamily(floatTypeFamily).setType(floatType);

    final Acceleration acceleration = new Acceleration()
      .setContext(new AccelerationContext()
        .setAnalysis(new DatasetAnalysis()
          .setColumnList(ImmutableList.of(
            create("field0", 10L, SqlTypeName.INTEGER),
            create("field1", 10L, SqlTypeName.FLOAT),
            create("field2", 10L, SqlTypeName.INTEGER),
            create("field3", 10L, SqlTypeName.FLOAT),
            create("field4", 10L, SqlTypeName.INTEGER),
            create("field5", 10L, SqlTypeName.FLOAT),
            create("field6", 10L, SqlTypeName.INTEGER),
            create("field7", 10L, SqlTypeName.FLOAT),
            create("field8", 10L, SqlTypeName.INTEGER),
            create("field9", 10L, SqlTypeName.FLOAT),
            create("field10", 10L, SqlTypeName.INTEGER),
            create("field11", 1000000L, SqlTypeName.INTEGER),
            create("field12", 1000000L, SqlTypeName.FLOAT))
          )
        ).setDatasetSchema(new RowType().setFieldList(ImmutableList.of(field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12)))
      );

    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS_TEMP);
    suggestor.writeAggregationLayouts(acceleration);

    // verify raw acceleration
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    final LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    final LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();

    assertEquals(3, details.getDimensionFieldList().size());
    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field2", details.getDimensionFieldList().get(1).getName());
    assertEquals("field4", details.getDimensionFieldList().get(2).getName());

    assertEquals(9, details.getMeasureFieldList().size());
    assertEquals("field12", details.getMeasureFieldList().get(0).getName());
    assertEquals("field11", details.getMeasureFieldList().get(1).getName());
    assertEquals("field10", details.getMeasureFieldList().get(2).getName());
    assertEquals("field9", details.getMeasureFieldList().get(3).getName());
    assertEquals("field8", details.getMeasureFieldList().get(4).getName());
    assertEquals("field7", details.getMeasureFieldList().get(5).getName());
    assertEquals("field6", details.getMeasureFieldList().get(6).getName());
    assertEquals("field5", details.getMeasureFieldList().get(7).getName());
    assertEquals("field3", details.getMeasureFieldList().get(8).getName());
  }
}
