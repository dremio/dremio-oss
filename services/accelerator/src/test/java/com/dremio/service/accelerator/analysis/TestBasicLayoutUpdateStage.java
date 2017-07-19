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
import static org.mockito.Mockito.when;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.pipeline.stages.BasicLayoutUpdateStage;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.ColumnStats;
import com.dremio.service.accelerator.proto.DatasetAnalysis;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.collect.ImmutableList;

/**
 * "True" unit tests for {@link BasicLayoutUpdateStage}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBasicLayoutUpdateStage {

  private static final int MAX_DIMENSION_FIELDS = 30;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StageContext context;

  private  ColumnStats create(String name, long cardinality, SqlTypeName type) {
    return new ColumnStats()
        .setField(new LayoutField()
            .setName(name)
            .setTypeFamily(type.getFamily().toString()))
        .setCardinality(cardinality);
  }

  private  LayoutFieldDescriptor create(String name) {
    return new LayoutFieldDescriptor().setName(name);
  }

  private void mockContext(Acceleration acceleration) {
    Mockito.reset(context);
    when(context.getCurrentAcceleration()).thenReturn(acceleration);
  }

  @Test
  public void testMeasuresAndDimensions () {
    // setup
    final String typeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String type = SqlTypeName.INTEGER.getName();
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
                )
            .setDatasetSchema(new RowType()
                .setFieldList(ImmutableList.of(
                    new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type)))
                )
            );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(2, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());
    assertEquals("field4", details.getMeasureFieldList().get(1).getName());

    LayoutId id = aggLayouts.getLayoutList().get(0).getId();
    int version = aggLayouts.getLayoutList().get(0).getVersion();

    //now set LogicalAggreation with a different set of dimensions and measures

    LogicalAggregationDescriptor logicalAggregation = new LogicalAggregationDescriptor()
        .setDimensionList(ImmutableList.of(
            create("field0"),
            create("field1"),
            create("field2"),
            create("field3"),
            create("field4")))
        .setMeasureList(ImmutableList.of(
            create("field5")));

    BasicLayoutUpdateStage stage = BasicLayoutUpdateStage.of(logicalAggregation);

    mockContext(acceleration);
    stage.execute(context);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(5, details.getDimensionFieldList().size());

    //make sure that specified dimension and measure list is returned
    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());
    assertEquals("field4", details.getDimensionFieldList().get(4).getName());

    assertEquals(1, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());

    //check that the id remains same and version is incremented
    assertEquals(id, aggLayouts.getLayoutList().get(0).getId());
    assertEquals(version + 1 , aggLayouts.getLayoutList().get(0).getVersion().intValue());
  }

  @Test
  public void testNonNumericMeasures () {
    // setup
    final String typeFamily = SqlTypeName.VARCHAR.getFamily().toString();
    final String type = SqlTypeName.VARCHAR.getName();
    final Acceleration acceleration = new Acceleration()
        .setContext(new AccelerationContext()
            .setAnalysis(new DatasetAnalysis()
                .setColumnList(ImmutableList.of(
                    create("field0", 10L, SqlTypeName.VARCHAR),
                    create("field1", 20L, SqlTypeName.VARCHAR),
                    create("field2", 1000L, SqlTypeName.VARCHAR),
                    create("field3", 10000L, SqlTypeName.VARCHAR),
                    create("field4", 10000L, SqlTypeName.VARCHAR),
                    create("field5", 1000000000L, SqlTypeName.VARCHAR))
                    )
                )
            .setDatasetSchema(new RowType()
                .setFieldList(ImmutableList.of(
                    new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type)))
                )
            );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(0, details.getMeasureFieldList().size());

    LayoutId id = aggLayouts.getLayoutList().get(0).getId();
    int version = aggLayouts.getLayoutList().get(0).getVersion();

    //now set LogicalAggreation with a different set of dimensions and measures

    LogicalAggregationDescriptor logicalAggregation = new LogicalAggregationDescriptor()
        .setDimensionList(ImmutableList.of(
            create("field0"),
            create("field1"),
            create("field2"),
            create("field3")))
        .setMeasureList(ImmutableList.of(
            create("field4"),
            create("field5")));

    BasicLayoutUpdateStage stage = BasicLayoutUpdateStage.of(logicalAggregation);

    mockContext(acceleration);
    stage.execute(context);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    //make sure that specified dimension and measure list is returned
    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(2, details.getMeasureFieldList().size());
    assertEquals("field4", details.getMeasureFieldList().get(0).getName());
    assertEquals("field5", details.getMeasureFieldList().get(1).getName());

    //check that the id remains same and version is incremented
    assertEquals(id, aggLayouts.getLayoutList().get(0).getId());
    assertEquals(version + 1 , aggLayouts.getLayoutList().get(0).getVersion().intValue());
  }

  @Test
  public void testMutuallyExclusiveMeasuresAndDimensions () {
    // setup
    final String typeFamily = SqlTypeName.INTEGER.getFamily().toString();
    final String type = SqlTypeName.INTEGER.getName();
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
                )
            .setDatasetSchema(new RowType()
                .setFieldList(ImmutableList.of(
                    new ViewFieldType().setName("field0").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field1").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field2").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field3").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field4").setTypeFamily(typeFamily).setType(type),
                    new ViewFieldType().setName("field5").setTypeFamily(typeFamily).setType(type)))
                )
            );

    // test
    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);
    suggestor.writeAggregationLayouts(acceleration);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    LayoutContainer aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    LayoutDetails details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(4, details.getDimensionFieldList().size());

    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());

    assertEquals(2, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());
    assertEquals("field4", details.getMeasureFieldList().get(1).getName());

    LayoutId id = aggLayouts.getLayoutList().get(0).getId();
    int version = aggLayouts.getLayoutList().get(0).getVersion();

    //now set LogicalAggreation with a different set of dimensions and measures

    LogicalAggregationDescriptor logicalAggregation = new LogicalAggregationDescriptor()
        .setDimensionList(ImmutableList.of(
            create("field0"),
            create("field1"),
            create("field2"),
            create("field3"),
            create("field4")))
        .setMeasureList(ImmutableList.of(
            create("field2"),
            create("field3"),
            create("field4"),
            create("field5")));

    BasicLayoutUpdateStage stage = BasicLayoutUpdateStage.of(logicalAggregation);

    mockContext(acceleration);
    stage.execute(context);

    // verify aggregation layout
    assertEquals(1, acceleration.getAggregationLayouts().getLayoutList().size());

    aggLayouts = acceleration.getAggregationLayouts();
    assertEquals(LayoutType.AGGREGATION, aggLayouts.getType());
    assertFalse(aggLayouts.getEnabled());

    details = aggLayouts.getLayoutList().get(0).getDetails();
    assertEquals(5, details.getDimensionFieldList().size());

    //make sure that specified dimension and measure list is returned
    assertEquals("field0", details.getDimensionFieldList().get(0).getName());
    assertEquals("field1", details.getDimensionFieldList().get(1).getName());
    assertEquals("field2", details.getDimensionFieldList().get(2).getName());
    assertEquals("field3", details.getDimensionFieldList().get(3).getName());
    assertEquals("field4", details.getDimensionFieldList().get(4).getName());

    assertEquals(1, details.getMeasureFieldList().size());
    assertEquals("field5", details.getMeasureFieldList().get(0).getName());

    //check that the id remains same and version is incremented
    assertEquals(id, aggLayouts.getLayoutList().get(0).getId());
    assertEquals(version + 1 , aggLayouts.getLayoutList().get(0).getVersion().intValue());
  }
}
