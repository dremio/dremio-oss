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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ConcurrentModificationException;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.LogicalPlanSerializer;
import com.dremio.exec.planner.acceleration.normalization.Normalizer;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.TypeValidators;
import com.dremio.service.accelerator.LayoutExpander;
import com.dremio.service.accelerator.analysis.AccelerationAnalyzer;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

/**
 * Unit tests planning stage
 */
@RunWith(MockitoJUnitRunner.class)
public class TestPlanningStage {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StageContext context;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private RelNode plan;

  @Test
  public void testPlanningStageNoLayouts() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    acceleration.setAggregationLayouts(null);
    acceleration.setRawLayouts(null);

    // test
    stage.execute(context);

    Assert.assertEquals(null, acceleration.getAggregationLayouts());
    Assert.assertEquals(null, acceleration.getRawLayouts());

    // verify
    verify(plan).getRowType(); // called once during mocking
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageAggLayoutOnly() {
    for (int i = 0; i < 4; ++i) {
      final MockedPojo pojo = new MockedPojo();
      final PlanningStage stage = getPlanningStage(pojo);

      // setup
      final Acceleration acceleration = getAndMockAcceleration(pojo);
      acceleration.setRawLayouts(null);

      // test
      stage.execute(context);

      Assert.assertEquals(false, pojo.aggLayout.getIncremental());
      Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
      Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
      Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());
      Assert.assertEquals(null, acceleration.getRawLayouts());

      // verify
      verify(pojo.getExpander()).expand(pojo.getAggLayout());
      verify(pojo.getAggNormalizer()).normalize(plan);
      verify(pojo.getSerializer()).serialize(plan);

      // These mocks aren't reset for each iteration of the loop, so increase the expected call count in accordance.
      verify(plan, times(2 * (i + 1) + i)).getRowType();
      verify(plan.getRowType(), times(i + 1)).getFieldList();
      verify(context, times(i + 1)).commit(acceleration);
    }
  }

  @Test
  public void testPlanningStageRawLayoutOnly() {
    for (int i = 0; i < 4; ++i) {
      final MockedPojo pojo = new MockedPojo();
      final PlanningStage stage = getPlanningStage(pojo);

      // setup
      final Acceleration acceleration = getAndMockAcceleration(pojo);
      acceleration.setAggregationLayouts(null);

      final boolean removeProject = 0 == (i & 1);
      final boolean enableMinMax = 0 == (i & 2);
      when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(removeProject);
      when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX)).thenReturn(enableMinMax);

      // test
      stage.execute(context);

      Assert.assertEquals(false, pojo.rawLayout.getIncremental());
      Assert.assertEquals(ByteString.EMPTY, pojo.rawLayout.getLogicalPlan());
      Assert.assertEquals(null, pojo.rawLayout.getRefreshField());
      Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.rawLayout.getLayoutSchema().getFieldList());
      Assert.assertEquals(null, acceleration.getAggregationLayouts());

      // verify
      verify(pojo.getExpander()).expand(pojo.getRawLayout());
      if (removeProject) {
        verify(pojo.getAggNormalizer()).normalize(plan);
      } else {
        verify(pojo.getRawNormalizer()).normalize(plan);
      }
      verify(pojo.getSerializer()).serialize(plan);

      // These mocks aren't reset for each iteration of the loop, so increase the expected call count in accordance.
      verify(plan, times(2 * (i + 1) + i)).getRowType();
      verify(plan.getRowType(), times(i + 1)).getFieldList();
      verify(context, times(i + 1)).commit(acceleration);
    }
  }

  @Test
  public void testPlanningStageRemoveProjectAndMinMaxDisabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(false);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX)).thenReturn(false);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getRawNormalizer()).normalize(plan);
    verify(pojo.getAggNormalizer()).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageWhenRemoveProjectDisabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(false);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    Assert.assertEquals(false, pojo.rawLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.rawLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.rawLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.rawLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getRawNormalizer()).normalize(plan);
    verify(pojo.getAggNormalizer()).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageWhenRemoveProjectEnabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(true);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getAggNormalizer(), times(2)).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageWhenRemoveProjectMinMaxEnabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(true);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX)).thenReturn(true);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getAggNormalizer(), times(2)).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageMinMaxEnabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(false);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX)).thenReturn(true);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    Assert.assertEquals(false, pojo.rawLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.rawLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.rawLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.rawLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getRawNormalizer()).normalize(plan);
    verify(pojo.getAggNormalizer()).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test
  public void testPlanningStageMinMaxDisabled() {
    final MockedPojo pojo = new MockedPojo();
    final PlanningStage stage = getPlanningStage(pojo);

    // setup
    final Acceleration acceleration = getAndMockAcceleration(pojo);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT)).thenReturn(false);
    when(pojo.getOptionManager().getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX)).thenReturn(false);

    // test
    stage.execute(context);

    Assert.assertEquals(false, pojo.aggLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.aggLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.aggLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.aggLayout.getLayoutSchema().getFieldList());

    Assert.assertEquals(false, pojo.rawLayout.getIncremental());
    Assert.assertEquals(ByteString.EMPTY, pojo.rawLayout.getLogicalPlan());
    Assert.assertEquals(null, pojo.rawLayout.getRefreshField());
    Assert.assertEquals(ImmutableList.<RelDataTypeField>of(), pojo.rawLayout.getLayoutSchema().getFieldList());

    // verify
    verify(pojo.getExpander()).expand(pojo.getRawLayout());
    verify(pojo.getExpander()).expand(pojo.getAggLayout());
    verify(pojo.getRawNormalizer()).normalize(plan);
    verify(pojo.getAggNormalizer()).normalize(plan);
    verify(pojo.getSerializer(), times(2)).serialize(plan);
    verify(plan, times(3)).getRowType(); // 3 because called once during mocking
    verify(plan.getRowType(), times(2)).getFieldList();
    verify(context).commit(acceleration);
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testPlanningStagePropagatesConcurrentModificationException() {
    // set up
    when(context.getCurrentAcceleration()).thenThrow(ConcurrentModificationException.class);

    // test
    final PlanningStage stage = new PlanningStage();
    stage.execute(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPlanningStagePropagatesIllegalArgumentException() {
    // set up
    OptionManager optManager = Mockito.mock(OptionManager.class);
    when(context.getOptionManager()).thenReturn(optManager);
    when(optManager.getOption(any(TypeValidators.BooleanValidator.class))).thenThrow(IllegalArgumentException.class);

    // test
    final PlanningStage stage = new PlanningStage();
    stage.execute(context);
  }

  private class MockedPojo {
    private Normalizer rawNormalizer;
    private Normalizer aggNormalizer;
    private AccelerationAnalyzer analyzer;
    private LogicalPlanSerializer serializer;
    private Layout rawLayout;
    private Layout aggLayout;

    private LayoutExpander expander;
    private OptionManager optionManager;

    public MockedPojo() {
      rawNormalizer = Mockito.mock(Normalizer.class);
      aggNormalizer = Mockito.mock(Normalizer.class);
      analyzer = Mockito.mock(AccelerationAnalyzer.class);
      serializer = Mockito.mock(LogicalPlanSerializer.class);
      rawLayout = new Layout().setLayoutType(LayoutType.RAW);
      aggLayout = new Layout().setLayoutType(LayoutType.AGGREGATION);
      expander = Mockito.mock(LayoutExpander.class);
      optionManager = Mockito.mock(OptionManager.class);
    }

    public void setRawNormalizer(Normalizer rawNormalizer) {
      this.rawNormalizer = rawNormalizer;
    }

    public Normalizer getRawNormalizer() {
      return rawNormalizer;
    }

    public void setAggNormalizer(Normalizer aggNormalizer) {
      this.aggNormalizer = aggNormalizer;
    }

    public Normalizer getAggNormalizer() {
      return aggNormalizer;
    }

    public AccelerationAnalyzer getAnalyzer() {
      return analyzer;
    }

    public LogicalPlanSerializer getSerializer() {
      return serializer;
    }

    public Layout getRawLayout() {
      return rawLayout;
    }

    public Layout getAggLayout() {
      return aggLayout;
    }

    public LayoutExpander getExpander() {
      return expander;
    }

    public OptionManager getOptionManager() {
      return optionManager;
    }

    public void setExpander(LayoutExpander expander) {
      this.expander = expander;
    }
  }

  private Acceleration getAndMockAcceleration(MockedPojo pojo) {
    final Acceleration acceleration = new Acceleration()
      .setContext(new AccelerationContext().setDataset(new DatasetConfig().setFullPathList(ImmutableList.of("a", "b"))))
      .setRawLayouts(new LayoutContainer().setLayoutList(ImmutableList.of(pojo.getRawLayout())))
      .setAggregationLayouts(new LayoutContainer().setLayoutList(ImmutableList.of(pojo.getAggLayout())));

    when(context.getCurrentAcceleration()).thenReturn(acceleration);
    when(context.getOptionManager()).thenReturn(pojo.getOptionManager());
    when(pojo.getAnalyzer().getPlan(any(NamespaceKey.class))).thenReturn(plan);

    when(plan.getRowType().getFieldList()).thenReturn(ImmutableList.<RelDataTypeField>of());
    when(plan.accept(any(RelShuttleImpl.class))).thenReturn(plan);

    when(pojo.getExpander().expand(any(Layout.class))).thenReturn(plan);
    when(pojo.getSerializer().serialize(any(RelNode.class))).thenReturn(ByteString.EMPTY_BYTE_ARRAY);

    when(pojo.getAggNormalizer().normalize(plan)).thenReturn(plan);
    when(pojo.getRawNormalizer().normalize(plan)).thenReturn(plan);
    return acceleration;
  }

  private PlanningStage getPlanningStage(final MockedPojo pojo) {
    return new PlanningStage() {
      @Override
      Normalizer getRawViewNormalizer(StageContext context) {
        return pojo.getRawNormalizer();
      }

      @Override
      Normalizer getAggregationViewNormalizer(StageContext context) {
        return pojo.getAggNormalizer();
      }

      @Override
      AccelerationAnalyzer createAnalyzer(final JobsService jobsService) {
        return pojo.getAnalyzer();
      }

      @Override
      LogicalPlanSerializer createSerializer(final RelOptCluster cluster) {
        return pojo.getSerializer();
      }

      @Override
      LayoutExpander createExpander(final RelNode view, boolean enableMinMax) {
        return pojo.getExpander();
      }
    };
  }
}
