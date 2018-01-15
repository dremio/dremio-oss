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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;

import org.apache.calcite.sql.type.SqlTypeFamily;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.exec.ExecConstants;
import com.dremio.service.accelerator.ChainExecutor;
import com.dremio.service.accelerator.DropTask;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Unit test activation stage.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestActivationStage {
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StageContext context;

  @Mock
  private MaterializationStore store;

  private String accelerationId = UUID.randomUUID().toString();

  /**
   * Holder for activation stage settings.
   */
  private final class Setting {
    private boolean isRawEnabled;
    private boolean isAggEnabled;
    private AccelerationState state;
    private AccelerationMode mode;

    Setting(boolean isRawEnabled, boolean isAggEnabled, AccelerationState stage, AccelerationMode mode) {
      this.isRawEnabled = isRawEnabled;
      this.isAggEnabled = isAggEnabled;
      state = stage;
      this.mode = mode;
    }
  }

  /**
   * Interface for testing activation stage.
   */
  private interface StageTester {
    void test(Setting setting);
  }

  @Test
  public void testEmptyAcceleration() {

    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        stage.execute(context);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        verify(context, times(4)).getCurrentAcceleration();
        verify(context).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggAccelerationNoCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);

        stage.execute(context);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, aggLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalRawAccelerationNoCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);

        stage.execute(context);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, rawLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggAndRawAccelerationNoCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);

        stage.execute(context);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, rawLayout);
        checkRemoved(currentAcceleration, aggLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testSameAggOriginalAndCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Acceleration currentAcceleration = context.getCurrentAcceleration();

        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);
        currentAcceleration.setAggregationLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getAggregationLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isAggEnabled) {
          checkRemoved(currentAcceleration, aggLayout);
        } else {
          verify(context.getExecutorService(), never()).execute(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testSameRawOriginalAndCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Acceleration currentAcceleration = context.getCurrentAcceleration();

        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);
        currentAcceleration.setRawLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getRawLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isRawEnabled) {
          checkRemoved(currentAcceleration, rawLayout);
        } else {
          verify(context.getExecutorService(), never()).execute(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testSameRawAndAggOriginalAndCurrentAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Acceleration currentAcceleration = context.getCurrentAcceleration();

        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);
        currentAcceleration.setRawLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getRawLayouts().getLayoutList())));

        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);
        currentAcceleration.setAggregationLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getAggregationLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isRawEnabled && !setting.isAggEnabled) {
          checkRemoved(currentAcceleration, rawLayout);
          checkRemoved(currentAcceleration, aggLayout);
        } else if (!setting.isRawEnabled) {
          checkRemoved(currentAcceleration, rawLayout);
          checkNotRemoved(currentAcceleration, aggLayout);
        } else if (!setting.isAggEnabled) {

          checkNotRemoved(currentAcceleration, rawLayout);
        } else {
          verify(context.getExecutorService(), never()).execute(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalRawThenCurrentAggAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(currentAcceleration, 0);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, rawLayout);
        checkNotRemoved(currentAcceleration, aggLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggThenCurrentRawAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(currentAcceleration, 0);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, aggLayout);
        checkNotRemoved(currentAcceleration, rawLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggAndRawThenCurrentRawAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        currentAcceleration.setAggregationLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getRawLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isRawEnabled) {
          checkRemoved(currentAcceleration, rawLayout);
        } else {
          checkNotRemoved(currentAcceleration, rawLayout);
        }

        checkRemoved(currentAcceleration, aggLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggAndRawThenCurrentAggAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        currentAcceleration.setAggregationLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getAggregationLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isAggEnabled) {
          checkRemoved(currentAcceleration, aggLayout);
        } else {
          checkNotRemoved(currentAcceleration, aggLayout);
        }

        checkRemoved(currentAcceleration, rawLayout);
        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalRawThenCurrentDifferentRawAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Optional<MaterializedLayout> rawLayout2 = addMaterializedRawLayout(currentAcceleration, 1);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, rawLayout);
        if (setting.isRawEnabled) {
          checkNotRemoved(currentAcceleration, rawLayout2);
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testOriginalAggThenCurrentDifferentAggAccelerations() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Optional<MaterializedLayout> aggLayout2 = addMaterializedAggLayout(currentAcceleration, 1);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        checkRemoved(currentAcceleration, aggLayout);
        if (setting.isAggEnabled) {
          checkNotRemoved(currentAcceleration, aggLayout2);
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testAggMaterialization() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Layout aggLayout = addAggLayout(currentAcceleration, 0);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(1)).getOriginalAcceleration();

        verify(context).commit(currentAcceleration);

        if (setting.isAggEnabled) {
          verify(context.getExecutorService()).submit(any(ChainExecutor.class));
        } else {
          verify(context.getExecutorService(), never()).submit(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testAggRawMaterialization() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Layout aggLayout = addAggLayout(currentAcceleration, 0);
        Layout rawLayout = addRawLayout(currentAcceleration, 0);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(1)).getOriginalAcceleration();

        verify(context).commit(currentAcceleration);

        if (setting.isAggEnabled || setting.isRawEnabled) {
          // If both are enabled, submit is still called once but with two chained tasks.
          verify(context.getExecutorService(), times(1)).submit(any(ChainExecutor.class));
        } else {
          verify(context.getExecutorService(), never()).submit(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test
  public void testRawMaterialization() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());

        Acceleration currentAcceleration = context.getCurrentAcceleration();
        Layout rawLayout = addRawLayout(currentAcceleration, 0);

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(1)).getOriginalAcceleration();

        verify(context).commit(currentAcceleration);

        if (setting.isRawEnabled) {
          verify(context.getExecutorService()).submit(any(ChainExecutor.class));
        } else {
          verify(context.getExecutorService(), never()).submit(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
  }

  @Test(expected = ConcurrentModificationException.class)
  public void testPropagatesConcurrentModificationException() {
    mockContext(context);

    // set up
    doThrow(ConcurrentModificationException.class).when(context).commit(any(Acceleration.class));

    // test
    final ActivationStage stage = ActivationStage.of();
    stage.execute(context);
  }

  @Test
  public void testAggAndRawDisabledOriginally() {
    testAllActivationStages(new StageTester() {
      @Override
      public void test(Setting setting) {
        mockContext(context);

        ActivationStage stage = ActivationStage.of(setting.isRawEnabled, setting.isAggEnabled, setting.state, setting.mode,
            ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getDefault().num_val.intValue());
        Acceleration originalAcceleration = context.getOriginalAcceleration();
        originalAcceleration.getAggregationLayouts().setEnabled(false);
        originalAcceleration.getRawLayouts().setEnabled(false);
        Acceleration currentAcceleration = context.getCurrentAcceleration();

        Optional<MaterializedLayout> aggLayout = addMaterializedAggLayout(originalAcceleration, 0);
        currentAcceleration.setAggregationLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getAggregationLayouts().getLayoutList())));

        Optional<MaterializedLayout> rawLayout = addMaterializedRawLayout(originalAcceleration, 0);
        currentAcceleration.setRawLayouts(
          new LayoutContainer().setLayoutList(ImmutableList.copyOf(originalAcceleration.getRawLayouts().getLayoutList())));

        stage.execute(context);

        verify(context, times(4)).getCurrentAcceleration();
        verify(context, times(2)).getOriginalAcceleration();
        verify(context).commit(currentAcceleration);

        if (!setting.isAggEnabled) {
          checkNotRemoved(currentAcceleration, aggLayout);
        } else {
          verify(context.getExecutorService(), never()).execute(any(ChainExecutor.class));
        }

        if (!setting.isRawEnabled) {
          checkNotRemoved(currentAcceleration, rawLayout);
        } else {
          verify(context.getExecutorService(), never()).submit(any(ChainExecutor.class));
        }

        checkAcceleration(setting, currentAcceleration);
      }
    });
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

    when(store.get(aggLayout.getId())).thenReturn(Optional.<MaterializedLayout>absent());

    return aggLayout;
  }

  private Optional<MaterializedLayout> addMaterializedAggLayout(Acceleration acceleration, int layoutNum) {
    Layout aggLayout = addAggLayout(acceleration, layoutNum);
    return addMaterializedAggLayout(acceleration, aggLayout, layoutNum);
  }

  private Optional<MaterializedLayout> addMaterializedAggLayout(Acceleration acceleration, Layout aggLayout, int layoutNum) {
    Optional<MaterializedLayout> layout = Optional.of(new MaterializedLayout(aggLayout.getId()));

    Materialization materialization = new Materialization(
      new MaterializationId("aggLayoutMaterialization" + layoutNum),
      aggLayout.getId()).setState(MaterializationState.DONE).setLayoutVersion(aggLayout.getVersion());
    layout.get().setMaterializationList(ImmutableList.of(materialization));

    when(store.get(aggLayout.getId())).thenReturn(layout);

    return layout;
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

    when(store.get(rawLayout.getId())).thenReturn(Optional.<MaterializedLayout>absent());

    return rawLayout;
  }

  private Optional<MaterializedLayout> addMaterializedRawLayout(Acceleration acceleration, int layoutNum) {
    Layout rawLayout = addRawLayout(acceleration, layoutNum);
    return addMaterializedRawLayout(acceleration, rawLayout, layoutNum);
  }

  private Optional<MaterializedLayout> addMaterializedRawLayout(Acceleration acceleration, Layout rawLayout, int layoutNum) {
    Optional<MaterializedLayout> layout = Optional.of(new MaterializedLayout(rawLayout.getId()));

    Materialization materialization = new Materialization(
      new MaterializationId("rawLayoutMaterialization" + layoutNum),
      rawLayout.getId()).setState(MaterializationState.DONE).setLayoutVersion(rawLayout.getVersion());
    layout.get().setMaterializationList(ImmutableList.of(materialization));

    when(store.get(rawLayout.getId())).thenReturn(layout);

    return layout;
  }

  private void checkAcceleration(Setting setting, Acceleration acceleration) {
    Assert.assertEquals(setting.mode, acceleration.getMode());
    Assert.assertEquals(setting.state, acceleration.getState());
    Assert.assertEquals(setting.isRawEnabled, acceleration.getRawLayouts().getEnabled());
    Assert.assertEquals(setting.isAggEnabled, acceleration.getAggregationLayouts().getEnabled());
  }

  private void checkNotRemoved(Acceleration acceleration, Optional<MaterializedLayout> materializedLayout) {
    verify(context.getExecutorService(), never()).execute(
      new DropTask(acceleration,
          materializedLayout.get().getMaterializationList().get(0),
          context.getJobsService(),
          context.getNamespaceService().findDatasetByUUID(acceleration.getId().getId())));
    verify(store, never()).remove(materializedLayout.get().getLayoutId());
  }

  private void checkRemoved(Acceleration acceleration, Optional<MaterializedLayout> materializedLayout) {
    verify(context.getExecutorService()).execute(
      new DropTask(acceleration,
          materializedLayout.get().getMaterializationList().get(0),
          context.getJobsService(),
          context.getNamespaceService().findDatasetByUUID(acceleration.getId().getId())));
    verify(store).remove(materializedLayout.get().getLayoutId());
  }

  private void mockContext(StageContext context) {
    Mockito.reset(this.context);
    Mockito.reset(store);

    DatasetConfig ds = new DatasetConfig()
      .setFullPathList(ImmutableList.of("a", "b"))
      .setId(new EntityId(accelerationId));
    // Set up an empty acceleration for the current.
    final Acceleration currentAcceleration = new Acceleration()
      .setId(new AccelerationId(accelerationId))
      .setContext(new AccelerationContext().setDataset(ds))
      .setRawLayouts(new LayoutContainer().setLayoutList(new ArrayList<Layout>()))
      .setAggregationLayouts(new LayoutContainer().setLayoutList(new ArrayList<Layout>()));

    // Set up an empty acceleration for the original.
    final Acceleration originalAcceleration = new Acceleration()
      .setContext(new AccelerationContext().setDataset(new DatasetConfig().setFullPathList(ImmutableList.of("a", "b"))))
      .setRawLayouts(new LayoutContainer()
          .setLayoutList(new ArrayList<Layout>())
          .setEnabled(true))
      .setAggregationLayouts(new LayoutContainer()
          .setLayoutList(new ArrayList<Layout>())
          .setEnabled(true));

    when(context.getAcceleratorStoragePlugin().getConfig().getPath()).thenReturn("acceleration");
    when(context.getOriginalAcceleration()).thenReturn(originalAcceleration);
    when(context.getCurrentAcceleration()).thenReturn(currentAcceleration);
    when(context.getMaterializationStore()).thenReturn(store);
    when(context.getAcceleratorStorageName()).thenReturn("__accelerator");
    when(context.getNamespaceService().findDatasetByUUID(currentAcceleration.getId().getId())).thenReturn(ds);
  }

  private void testAllActivationStages(StageTester command) {
    List<Setting> possiblePartialSettings = new ArrayList<>();

    // Set up possible settings, excluding AccelerationState values.
    possiblePartialSettings.add(new Setting(false, false, AccelerationState.DISABLED, AccelerationMode.AUTO));
    possiblePartialSettings.add(new Setting(false, false, AccelerationState.DISABLED, AccelerationMode.MANUAL));
    possiblePartialSettings.add(new Setting(false, true, AccelerationState.DISABLED, AccelerationMode.AUTO));
    possiblePartialSettings.add(new Setting(false, true, AccelerationState.DISABLED, AccelerationMode.MANUAL));
    possiblePartialSettings.add(new Setting(true, false, AccelerationState.DISABLED, AccelerationMode.AUTO));
    possiblePartialSettings.add(new Setting(true, false, AccelerationState.DISABLED, AccelerationMode.MANUAL));
    possiblePartialSettings.add(new Setting(true, true, AccelerationState.DISABLED, AccelerationMode.AUTO));
    possiblePartialSettings.add(new Setting(true, true, AccelerationState.DISABLED, AccelerationMode.MANUAL));

    // Now cross the above with the acceleration states to arrive at the full set of possible settings.
    for (AccelerationState state : AccelerationState.values()) {
      for (Setting setting : possiblePartialSettings) {
        command.test(new Setting(setting.isRawEnabled, setting.isAggEnabled, state, setting.mode));
      }
    }
  }
}
