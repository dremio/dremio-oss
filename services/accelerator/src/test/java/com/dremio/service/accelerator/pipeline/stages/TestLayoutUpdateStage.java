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
package com.dremio.service.accelerator.pipeline.stages;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests layout update stage
 */
@RunWith(MockitoJUnitRunner.class)
public class TestLayoutUpdateStage {
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StageContext context;

  @Test
  public void testNoLayouts() {
    mockContext(context);

    LayoutUpdateStage stage = LayoutUpdateStage.of(ImmutableList.<LayoutDescriptor>of(), ImmutableList.<LayoutDescriptor>of());
    stage.execute(context);

    verify(context).commit(context.getCurrentAcceleration());
  }

  @Test
  public void testRawLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout rawLayout = addRawLayout(curAcceleration, 0);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of(createRawLayoutDesc(rawLayout));
    List<LayoutDescriptor> aggLayouts = ImmutableList.of();

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.assertEquals(AccelerationMode.MANUAL, curAcceleration.getMode());
    Assert.assertEquals(ImmutableList.of(), curAcceleration.getAggregationLayouts().getLayoutList());
    Assert.assertEquals(ImmutableList.of(rawLayout), curAcceleration.getRawLayouts().getLayoutList());

    verify(context, times(2)).getCurrentAcceleration();
    verify(context).commit(curAcceleration);
  }

  @Test
  public void testAggLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout aggLayout = addAggLayout(curAcceleration, 0);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of();
    List<LayoutDescriptor> aggLayouts = ImmutableList.of(createAggLayoutDesc(aggLayout));

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.assertEquals(AccelerationMode.MANUAL, curAcceleration.getMode());
    Assert.assertEquals(ImmutableList.of(aggLayout), curAcceleration.getAggregationLayouts().getLayoutList());
    Assert.assertEquals(ImmutableList.of(), curAcceleration.getRawLayouts().getLayoutList());

    verify(context, times(2)).getCurrentAcceleration();
    verify(context).commit(curAcceleration);
  }

  @Test
  public void testNotAddedAggLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout aggLayout = addAggLayout(curAcceleration, 0);
    curAcceleration.getAggregationLayouts().getLayoutList().remove(aggLayout);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of();
    List<LayoutDescriptor> aggLayouts = ImmutableList.of(createAggLayoutDesc(aggLayout));

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.assertEquals(AccelerationMode.MANUAL, curAcceleration.getMode());
    Assert.assertEquals(ImmutableList.of(), curAcceleration.getRawLayouts().getLayoutList());

    Layout genAggLayout = curAcceleration.getAggregationLayouts().getLayoutList().get(0);
    Assert.assertEquals(aggLayout.getDetails().getDimensionFieldList(), genAggLayout.getDetails().getDimensionFieldList());
    Assert.assertEquals(aggLayout.getDetails().getMeasureFieldList(), genAggLayout.getDetails().getMeasureFieldList());

    verify(context, times(2)).getCurrentAcceleration();
    verify(context).commit(curAcceleration);
  }

  @Test
  public void testNotAddedRawLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout rawLayout = addRawLayout(curAcceleration, 0);
    curAcceleration.getRawLayouts().getLayoutList().remove(rawLayout);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of(createRawLayoutDesc(rawLayout));
    List<LayoutDescriptor> aggLayouts = ImmutableList.of();

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.assertEquals(AccelerationMode.MANUAL, curAcceleration.getMode());
    Assert.assertEquals(ImmutableList.of(), curAcceleration.getAggregationLayouts().getLayoutList());

    Layout genRawLayout = curAcceleration.getRawLayouts().getLayoutList().get(0);
    Assert.assertEquals(rawLayout.getDetails().getDisplayFieldList(), genRawLayout.getDetails().getDisplayFieldList());

    verify(context, times(2)).getCurrentAcceleration();
    verify(context).commit(curAcceleration);
  }

  @Test
  public void testAggAndRawLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout aggLayout = addAggLayout(curAcceleration, 0);
    Layout rawLayout = addRawLayout(curAcceleration, 0);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of(createRawLayoutDesc(rawLayout));
    List<LayoutDescriptor> aggLayouts = ImmutableList.of(createAggLayoutDesc(aggLayout));

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.assertEquals(AccelerationMode.MANUAL, curAcceleration.getMode());
    Assert.assertEquals(ImmutableList.of(aggLayout), curAcceleration.getAggregationLayouts().getLayoutList());
    Assert.assertEquals(ImmutableList.of(rawLayout), curAcceleration.getRawLayouts().getLayoutList());

    verify(context, times(2)).getCurrentAcceleration();
    verify(context).commit(curAcceleration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRawLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout rawLayout = addRawLayout(curAcceleration, 1);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of(createRawLayoutDesc(rawLayout));
    List<LayoutDescriptor> aggLayouts = ImmutableList.of();

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);

    Assert.fail("Exception should have been thrown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAggLayouts() {
    mockContext(context);

    Acceleration curAcceleration = context.getCurrentAcceleration();
    Layout aggLayout = addAggLayout(curAcceleration, 1);

    List<LayoutDescriptor> rawLayouts = ImmutableList.of();
    List<LayoutDescriptor> aggLayouts = ImmutableList.of(createAggLayoutDesc(aggLayout));

    LayoutUpdateStage stage = LayoutUpdateStage.of(rawLayouts, aggLayouts);
    stage.execute(context);
  }

  private void mockContext(StageContext context) {
    Mockito.reset(context);

    RowType schema = new RowType();
    schema.setFieldList(ImmutableList.of(
      new ViewFieldType("dim0", SqlTypeName.CHAR.getName()).setTypeFamily(SqlTypeFamily.CHARACTER.toString()),
      new ViewFieldType("measure0", SqlTypeName.DOUBLE.getName()).setTypeFamily(SqlTypeFamily.NUMERIC.toString())
    ));

    // Set up an empty acceleration for the original.
    final Acceleration currentAcceleration = new Acceleration()
      .setContext(new AccelerationContext().setDataset(new DatasetConfig().setFullPathList(ImmutableList.of("a", "b"))).setDatasetSchema(schema))
      .setRawLayouts(new LayoutContainer().setLayoutList(new ArrayList<Layout>()))
      .setAggregationLayouts(new LayoutContainer().setLayoutList(new ArrayList<Layout>()));

    when(context.getCurrentAcceleration()).thenReturn(currentAcceleration);
  }

  private Layout addAggLayout(Acceleration acceleration, int layoutNum) {
    List<LayoutDimensionField> dimFields = new ArrayList<>();
    dimFields.add(new LayoutDimensionField().setName("dim" + layoutNum).setTypeFamily(SqlTypeFamily.CHARACTER.toString()));
    List<LayoutField> measureFields = new ArrayList<>();
    measureFields.add(new LayoutField().setName("measure" + layoutNum).setTypeFamily(SqlTypeFamily.NUMERIC.toString()));

    Layout aggLayout = new Layout()
      .setLayoutType(LayoutType.AGGREGATION)
      .setDetails(new LayoutDetails().setDimensionFieldList(dimFields).setMeasureFieldList(measureFields))
      .setId(new LayoutId("aggLayout" + layoutNum))
      .setName("aggLayout")
      .setVersion(0);

    acceleration.getAggregationLayouts().getLayoutList().add(aggLayout);

    return aggLayout;
  }

  private Layout addRawLayout(Acceleration acceleration, int layoutNum) {
    List<LayoutField> measureFields = new ArrayList<>();
    measureFields.add(new LayoutField().setName("dim" + layoutNum).setTypeFamily(SqlTypeFamily.CHARACTER.toString()));
    measureFields.add(new LayoutField().setName("measure" + layoutNum).setTypeFamily(SqlTypeFamily.NUMERIC.toString()));

    Layout rawLayout = new Layout()
      .setLayoutType(LayoutType.RAW)
      .setDetails(new LayoutDetails().setDisplayFieldList(measureFields))
      .setId(new LayoutId("rawLayout" + layoutNum))
      .setName("rawLayout")
      .setVersion(0);

    acceleration.getRawLayouts().getLayoutList().add(rawLayout);

    return rawLayout;
  }

  private LayoutDescriptor createAggLayoutDesc(Layout layout) {
    LayoutDetailsDescriptor detailsDesc = new LayoutDetailsDescriptor();
    detailsDesc.setDimensionFieldList(new ArrayList<LayoutDimensionFieldDescriptor>());
    detailsDesc.setMeasureFieldList(new ArrayList<LayoutFieldDescriptor>());

    for (LayoutDimensionField field : layout.getDetails().getDimensionFieldList()) {
      detailsDesc.getDimensionFieldList().add(new LayoutDimensionFieldDescriptor(field.getName()));
    }

    for (LayoutField field : layout.getDetails().getMeasureFieldList()) {
      detailsDesc.getMeasureFieldList().add(new LayoutFieldDescriptor(field.getName()));
    }

    return new LayoutDescriptor(detailsDesc).setId(layout.getId()).setName(layout.getName());
  }

  private LayoutDescriptor createRawLayoutDesc(Layout layout) {
    LayoutDetailsDescriptor detailsDesc = new LayoutDetailsDescriptor();
    detailsDesc.setDisplayFieldList(new ArrayList<LayoutFieldDescriptor>());
    for (LayoutField field : layout.getDetails().getDisplayFieldList()) {
      detailsDesc.getDisplayFieldList().add(new LayoutFieldDescriptor(field.getName()));
    }

    return new LayoutDescriptor(detailsDesc).setId(layout.getId()).setName(layout.getName());
  }
}
