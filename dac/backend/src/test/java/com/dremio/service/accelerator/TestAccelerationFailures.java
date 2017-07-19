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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDetailsApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDimensionFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutFieldApiDescriptor;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.accelerator.pipeline.Pipeline;
import com.dremio.service.accelerator.pipeline.PipelineFactory;
import com.dremio.service.accelerator.pipeline.PipelineManager;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.pipeline.StageException;
import com.dremio.service.accelerator.pipeline.stages.ActivationStage;
import com.dremio.service.accelerator.pipeline.stages.AnalysisStage;
import com.dremio.service.accelerator.pipeline.stages.BasicLayoutUpdateStage;
import com.dremio.service.accelerator.pipeline.stages.LayoutUpdateStage;
import com.dremio.service.accelerator.pipeline.stages.PlanningStage;
import com.dremio.service.accelerator.pipeline.stages.SuggestionStage;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Test acceleration when stages fail.
 */
public class TestAccelerationFailures extends AccelerationTestUtil {

  private static boolean failAnalysisStage = false;
  private static boolean failSuggestionStage = false;
  private static boolean failUpdateLayoutStage = false;
  private static boolean failBasicLayoutUpdateStage = false;
  private static boolean failPlanningStage = false;
  private static boolean failActivationStage = false;
  private static byte[] employeeJsonData;
  private static byte[] employeeWithNullJsonData;

  private static final LayoutApiDescriptor aggregationLayout = new LayoutApiDescriptor().setDetails(
    new LayoutDetailsApiDescriptor()
      .setDimensionFieldList(Lists.newArrayList(new LayoutDimensionFieldApiDescriptor("full_name").setGranularity(DimensionGranularity.NORMAL)))
      .setMeasureFieldList(Lists.newArrayList(new LayoutFieldApiDescriptor("rating"))));
  private static final LayoutApiDescriptor emptyAggregationLayout = new LayoutApiDescriptor().setDetails(
    new LayoutDetailsApiDescriptor()
      .setDimensionFieldList(Lists.<LayoutDimensionFieldApiDescriptor>newArrayList())
      .setMeasureFieldList(Lists.<LayoutFieldApiDescriptor>newArrayList()));
  private static final LayoutApiDescriptor dummyAggregationLayout = new LayoutApiDescriptor().setDetails(
    new LayoutDetailsApiDescriptor()
      .setDimensionFieldList(Lists.newArrayList(new LayoutDimensionFieldApiDescriptor("rating").setGranularity(DimensionGranularity.NORMAL)))
      .setMeasureFieldList(Lists.newArrayList(new LayoutFieldApiDescriptor("rating"))));
  private static final LayoutApiDescriptor rawLayout = new LayoutApiDescriptor().setDetails(
    new LayoutDetailsApiDescriptor()
      .setDisplayFieldList(Lists.newArrayList(new LayoutFieldApiDescriptor("employee_id"))));
  private static final LayoutApiDescriptor emptyRawLayout = new LayoutApiDescriptor().setDetails(
    new LayoutDetailsApiDescriptor()
      .setDisplayFieldList(Lists.<LayoutFieldApiDescriptor>newArrayList()));

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    BaseTestServer.init();

    // Add custom PipelineFactory that replaces each stage with dummy stages that can fail.
    final AccelerationServiceImpl service = (AccelerationServiceImpl) l(AccelerationService.class);
    Field pipeManagerField = service.getClass().getDeclaredField("pipelineManager");
    pipeManagerField.setAccessible(true);
    PipelineManager pipeManager = (PipelineManager) pipeManagerField.get(service);
    Field pipeFactoryField = pipeManager.getClass().getDeclaredField("factory");
    pipeFactoryField.setAccessible(true);
    pipeFactoryField.set(pipeManager, new TestPipelineFactory());
  }

  private static boolean created = false;

  @Before
  public void setup() throws Exception {
    if (created) {
      return;
    }
    created = true;

    addCPSource();
    addEmployeesJson();

    final DatasetPath empWithNull = new DatasetPath(Arrays.asList(TEST_SOURCE, "employees_with_null.json"));
    final DatasetPath empWithNullVDS = new DatasetPath(Arrays.asList(TEST_SPACE, "ds_emp_wn"));
    addJson(empWithNull, empWithNullVDS);

    employeeJsonData = readTestJson(EMPLOYEES_FILE);
    employeeWithNullJsonData = readTestJson(EMPLOYEES_WITH_NULL_FILE);
  }

  @AfterClass
  public static void clearAcceleration() throws Exception {
    getAccelerationService().developerService().clearAllAccelerations();
  }

  @After
  public void resetStages() throws Exception {
    failAnalysisStage = false;
    failSuggestionStage = false;
    failPlanningStage = false;
    failActivationStage = false;
  }

  @Test
  public void testAnalysisFailureAndRecovery() throws Exception {
    failAnalysisStage = true;
    final AccelerationApiDescriptor newApiDescriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);

    pollAccelerationErrorState(newApiDescriptor.getId());

    // once in manual mode, should stay there
    failAnalysisStage = false;
    pollAccelerationErrorState(newApiDescriptor.getId());
    pollAccelerationErrorState(newApiDescriptor.getId());

    failAnalysisStage = true;
    pollAccelerationErrorState(newApiDescriptor.getId());
    final AccelerationApiDescriptor failed = pollAccelerationErrorState(newApiDescriptor.getId());

    // Now just add some dummy columns to see what happens if we modify the aggregation layout incorrectly.
    setManualModeLayout(failed);
    saveAccelerationFailure(failed.getId(), failed);
    final AccelerationApiDescriptor failed2 = pollAccelerationErrorState(newApiDescriptor.getId());

    setManualModeLayout(failed2);
    failAnalysisStage = false;
    final AccelerationApiDescriptor saved = saveAcceleration(failed2.getId(), failed2);
    waitForMaterialization(saved.getId(), true);

    List<Materialization> materializations = getMaterializations(saved.getId());
    assertTrue(!materializations.isEmpty());

    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved.getId());
    checkManualModeLayout(afterMaterialize);
  }

  @Test
  public void testSuggestionFailureAndRecovery() throws Exception {
    // suggestion stage should fail
    failSuggestionStage = true;
    final AccelerationApiDescriptor newApiDescriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);

    boolean layoutFinished = false;
    AccelerationApiDescriptor layout = null;
    while (!layoutFinished) {
      layout = pollAcceleration(newApiDescriptor.getId());
      layoutFinished = layout.getState() != AccelerationStateApiDescriptor.NEW;
      Thread.sleep(100);
    }
    checkAccelerationErrorState(layout, true);

    // Go into manual mode and set some layouts and save
    setManualModeLayout(layout);
    final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    waitForMaterialization(saved.getId(), true);

    List<Materialization> materializations = getMaterializations(saved.getId());
    assertTrue(!materializations.isEmpty());

    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved.getId());
    checkManualModeLayout(afterMaterialize);
  }

  @Test
  public void testUpdateFailureAndRecoveryNoLayoutChange() throws Exception {
    failUpdateLayoutStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(true, false, true);
    // now re-enable planning stage
    failUpdateLayoutStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testUpdateFailureAndRecovery() throws Exception {
    failUpdateLayoutStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(false, false, true);
    // now re-enable planning stage
    failUpdateLayoutStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testBasicUpdateFailureAndRecoveryNoLayoutChange() throws Exception {
    failBasicLayoutUpdateStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(true, false, false);
    // now re-enable planning stage
    failBasicLayoutUpdateStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testBasicUpdateFailureAndRecovery() throws Exception {
    failBasicLayoutUpdateStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(false, false, false);
    // now re-enable planning stage
    failBasicLayoutUpdateStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testPlanningFailureAndRecoveryNoLayoutChange() throws Exception {
    // create a new acceleration but planning will fail
    failPlanningStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(true, true, true);
    // now re-enable planning stage
    failPlanningStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testPlanningFailureAndRecovery() throws Exception {
    // create a new acceleration but planning will fail
    failPlanningStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(false, true, true);
    // now re-enable planning stage
    failPlanningStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testPlanningSuccessThenFailureAndRecovery() throws Exception {
    // Test success.
    final AccelerationApiDescriptor newApiDescriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    setManualModeLayout(layout);
    final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    assertEquals(AccelerationStateApiDescriptor.ENABLED, saved.getState());

    waitForMaterialization(saved.getId(), true);
    AccelerationApiDescriptor materialize = pollAcceleration(saved.getId());
    List<Materialization> materializations = getMaterializations(materialize.getId());
    assertFalse(materializations.isEmpty());

    // Then failure.
    failPlanningStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(
      EMPLOYEES_WITH_NULL_FILE,
      false,
      true,
      true);
    failPlanningStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testPlanningAggOneLayoutFailureAndRecovery() throws Exception {
    // Test one good and one bad aggregate layout, ensure layout is not saved and then
    // save with just the good layout.
    failPlanningStage = true;
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    checkAccelerationErrorState(layout, false);
    failPlanningStage = false;

    List<LayoutApiDescriptor> oldAggLayouts = layout.getAggregationLayouts().getLayoutList();
    List<LayoutApiDescriptor> oldRawLayouts = layout.getRawLayouts().getLayoutList();

    setManualModeLayout(layout);
    layout.getAggregationLayouts().getLayoutList().add(new LayoutApiDescriptor().setDetails(
      new LayoutDetailsApiDescriptor()
        .setDimensionFieldList(Lists.newArrayList(new LayoutDimensionFieldApiDescriptor("full_name").setGranularity(DimensionGranularity.NORMAL)))
        .setMeasureFieldList(Lists.newArrayList(new LayoutFieldApiDescriptor("badColumn")))));

    AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    AccelerationApiDescriptor failed = waitForFail(saved);

    // Should still be failing, nothing has changed for the original layouts.
    assertEquals(AccelerationStateApiDescriptor.ERROR, failed.getState());
    assertEquals(AccelerationMode.MANUAL, failed.getMode());
    assertEquals(oldAggLayouts, failed.getAggregationLayouts().getLayoutList());
    assertEquals(oldRawLayouts, failed.getRawLayouts().getLayoutList());

    // Change the layouts to a valid set.
    setManualModeLayout(failed);
    failed.getRawLayouts().setLayoutList(Lists.<LayoutApiDescriptor>newArrayList());

    // Re-save the acceleration.
    AccelerationApiDescriptor saved2 = saveAcceleration(failed.getId(), failed);
    assertEquals(AccelerationStateApiDescriptor.ENABLED, saved2.getState());
    waitForMaterialization(saved2.getId(), true);

    // make sure we have at least one materialization!
    List<Materialization> materializations = getMaterializations(saved2.getId());
    assertTrue(!materializations.isEmpty());

    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved2.getId());

    // Verify things have changed.
    assertEquals(true, afterMaterialize.getAggregationLayouts().getEnabled());
    assertEquals(false, afterMaterialize.getRawLayouts().getEnabled());
    assertEquals(AccelerationMode.MANUAL, afterMaterialize.getMode());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().size() == 1);
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getPartitionFieldList() == null
      || afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getPartitionFieldList().isEmpty());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList() == null
      || afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList().isEmpty());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getSortFieldList() == null
      || afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getSortFieldList().isEmpty());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDistributionFieldList() == null
      || afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDistributionFieldList().isEmpty());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getMeasureFieldList().equals(
      aggregationLayout.getDetails().getMeasureFieldList()));
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDimensionFieldList().equals(
      aggregationLayout.getDetails().getDimensionFieldList()));
    assertTrue(afterMaterialize.getRawLayouts().getLayoutList().isEmpty());
  }

  @Test
  public void testPlanningRawOneBadLayoutFailureAndRecovery() throws Exception {
    // Test one good and one bad raw layout, ensure layout is not saved and then
    // save with just the good layout.
    failPlanningStage = true;
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    checkAccelerationErrorState(layout, false);
    failPlanningStage = false;

    List<LayoutApiDescriptor> oldAggLayouts = layout.getAggregationLayouts().getLayoutList();
    List<LayoutApiDescriptor> oldRawLayouts = layout.getRawLayouts().getLayoutList();

    layout.getAggregationLayouts().setLayoutList(Lists.<LayoutApiDescriptor>newArrayList());
    layout.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout, new LayoutApiDescriptor().setDetails(
      new LayoutDetailsApiDescriptor()
        .setDisplayFieldList(Lists.newArrayList(new LayoutFieldApiDescriptor("badColumn"))))));
    layout.getRawLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    AccelerationApiDescriptor failed = waitForFail(saved);

    // Should still be failing, nothing has changed for the original layouts.
    assertEquals(AccelerationStateApiDescriptor.ERROR, failed.getState());
    assertEquals(AccelerationMode.MANUAL, failed.getMode());
    assertEquals(oldAggLayouts, failed.getAggregationLayouts().getLayoutList());
    assertEquals(oldRawLayouts, failed.getRawLayouts().getLayoutList());

    // Change the layouts to a valid set.
    failed.getAggregationLayouts().setLayoutList(Lists.<LayoutApiDescriptor>newArrayList());
    failed.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout));
    failed.getRawLayouts().setEnabled(true);
    failed.setMode(AccelerationMode.MANUAL);

    // Re-save the acceleration.
    AccelerationApiDescriptor saved2 = saveAcceleration(failed.getId(), failed);
    assertEquals(AccelerationStateApiDescriptor.ENABLED, saved2.getState());
    waitForMaterialization(saved2.getId(), true);

    // make sure we have at least one materialization!
    List<Materialization> materializations = getMaterializations(saved2.getId());
    assertTrue(!materializations.isEmpty());

    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved2.getId());

    // Verify things have changed.
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().isEmpty());
    assertTrue(afterMaterialize.getRawLayouts().getEnabled());
    assertEquals(1, afterMaterialize.getRawLayouts().getLayoutList().size());
    assertTrue(afterMaterialize.getRawLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList().equals(rawLayout.getDetails().getDisplayFieldList()));
  }

  @Test
  public void testNoLayoutsFail() throws Exception {
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    layout.getAggregationLayouts().setLayoutList(Lists.<LayoutApiDescriptor>newArrayList());
    layout.getAggregationLayouts().setEnabled(true);
    layout.getRawLayouts().setLayoutList(Lists.<LayoutApiDescriptor>newArrayList());
    layout.getRawLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    try {
      saveAcceleration(layout.getId(), layout);
      Assert.fail("expected validation failure");
    } catch (AssertionError e) {
      Assert.assertTrue("incorrect error", e.getLocalizedMessage().contains("descriptor.aggregationLayouts.layoutList is required"));
    }
  }

  @Test
  public void testRawOneEmptyLayoutFail() throws Exception {
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    layout.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout, emptyRawLayout));
    layout.getRawLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    try {
      saveAcceleration(layout.getId(), layout);
      Assert.fail("expected validation failure");
    } catch (AssertionError e) {
      Assert.assertTrue("incorrect error", e.getLocalizedMessage().contains("details.displayFieldList is required"));
    }
  }

  @Test
  public void testAggOneEmptyLayoutFail() throws Exception {
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    layout.getAggregationLayouts().setLayoutList(Lists.newArrayList(aggregationLayout, emptyAggregationLayout));
    layout.getAggregationLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    try {
      saveAcceleration(layout.getId(), layout);
      Assert.fail("expected validation failure");
    } catch (AssertionError e) {
      Assert.assertTrue("incorrect error", e.getLocalizedMessage().contains("details.dimensionFieldList is required"));
    }
  }

  @Test
  public void testActivationFailureAndRecoveryNoLayoutChange() throws Exception {
    failActivationStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(true, true, true);
    // now re-enable planning stage
    failActivationStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testActivationFailureAndRecovery() throws Exception {
    failActivationStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(false, true, true);
    // now re-enable planning stage
    failActivationStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testActivationSuccessThenFailureAndRecovery() throws Exception {
    // Test success.
    final AccelerationApiDescriptor newApiDescriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    setManualModeLayout(layout);
    final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    assertEquals(AccelerationStateApiDescriptor.ENABLED, saved.getState());

    waitForMaterialization(saved.getId(), true);
    AccelerationApiDescriptor materialize = pollAcceleration(saved.getId());
    List<Materialization> materializations = getMaterializations(materialize.getId());
    assertFalse(materializations.isEmpty());

    // Then failure.
    failActivationStage = true;
    AccelerationApiDescriptor failed = testFailureAndRecoveryHelper(
      EMPLOYEES_WITH_NULL_FILE,
      false,
      true,
      true);
    failActivationStage = false;
    testFailureAndRecoveryHelperAfterResetFailure(failed);
  }

  @Test
  public void testAddAccelerationTwice() throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(EMPLOYEES);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(newApiDescriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());
    setManualModeLayout(layout);

    try {
      createNewAcceleration(EMPLOYEES);
      Assert.fail("duplicate acceleration should throw");
    } catch (AssertionError e) {
      Assert.assertTrue("Unexpected error message", e.getLocalizedMessage().contains("acceleration exists"));
    }
  }

  @Ignore("DX-6995")
  @Test
  public void testAddSameRawLayoutTwice() throws Exception {
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    layout.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout, rawLayout));
    layout.getRawLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    waitForFail(saved);
  }

  @Ignore("DX-6995")
  @Test
  public void testAddSameAggLayoutTwice() throws Exception {
    final AccelerationApiDescriptor descriptor = createAccelerationOnNewTable(EMPLOYEES_FILE);
    final AccelerationApiDescriptor layout = waitForLayoutGeneration(descriptor.getId());
    assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
    assertEquals(AccelerationMode.AUTO, layout.getMode());

    layout.getAggregationLayouts().setLayoutList(Lists.newArrayList(aggregationLayout, aggregationLayout));
    layout.getAggregationLayouts().setEnabled(true);
    layout.setMode(AccelerationMode.MANUAL);

    final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
    waitForFail(saved);
  }

  private AccelerationApiDescriptor testFailureAndRecoveryHelper(boolean sameLayout, boolean genLayoutFail, boolean testManualMode) throws Exception {
    return testFailureAndRecoveryHelper(EMPLOYEES_FILE, sameLayout, genLayoutFail, testManualMode);
  }

  private AccelerationApiDescriptor testFailureAndRecoveryHelper(
      String datasetFile, boolean sameLayout, boolean genLayoutFail, boolean testManualMode) throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createAccelerationOnNewTable(datasetFile);

    final AccelerationApiDescriptor layout = waitForLayoutGeneration(newApiDescriptor.getId());
    if (genLayoutFail) {
      checkAccelerationErrorState(layout, false);
    } else {
      assertEquals(AccelerationStateApiDescriptor.DISABLED, layout.getState());
      assertEquals(AccelerationMode.AUTO, layout.getMode());
    }

    if (testManualMode) {
      // manual mode:  set a new layout
      if (sameLayout) {
        setManualModeLayout(layout);
      } else {
        setManualDummyModeLayout(layout);
      }
      final AccelerationApiDescriptor saved = saveAcceleration(layout.getId(), layout);
      assertEquals(AccelerationStateApiDescriptor.ENABLED, saved.getState());

      // manual mode:  wait for failure
      return waitForFail(saved);
    } else {
      return newApiDescriptor;
    }
  }

  private AccelerationApiDescriptor waitForFail(AccelerationApiDescriptor saved) throws InterruptedException {
    boolean saveFailed = false;
    int iteration = 0;
    AccelerationApiDescriptor poll = null;
    while (iteration < 80) {
      final AccelerationApiDescriptor pollTemp = pollAcceleration(saved.getId());
      if (pollTemp.getState() == AccelerationStateApiDescriptor.ERROR) {
        saveFailed = true;
        poll = pollTemp;
        break;
      }
      Thread.sleep(100);
      ++iteration;
    }
    assertTrue(saveFailed);
    checkAccelerationErrorState(poll, false);
    return poll;
  }

  private void testFailureAndRecoveryHelperAfterResetFailure(AccelerationApiDescriptor poll) throws Exception {
    setManualModeLayout(poll);
    final AccelerationApiDescriptor saved2 = saveAcceleration(poll.getId(), poll);
    assertEquals(AccelerationStateApiDescriptor.ENABLED, saved2.getState());
    waitForMaterialization(saved2.getId(), true);

    // make sure we have at least one materialization!
    List<Materialization> materializations = getMaterializations(saved2.getId());
    assertTrue(!materializations.isEmpty());

    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved2.getId());
    checkManualModeLayout(afterMaterialize);
  }

  private AccelerationApiDescriptor pollAccelerationErrorState(AccelerationId id) {
    final AccelerationApiDescriptor descriptor = pollAcceleration(id);
    checkAccelerationErrorState(descriptor, true);
    return descriptor;
  }

  private void checkAccelerationErrorState(final AccelerationApiDescriptor descriptor, final boolean emptyLayouts) {
    assertEquals(AccelerationStateApiDescriptor.ERROR, descriptor.getState());
    assertEquals(AccelerationMode.MANUAL, descriptor.getMode());
    assertFalse(descriptor.getRawLayouts().getEnabled());
    if (emptyLayouts) {
      assertTrue(descriptor.getRawLayouts().getLayoutList().isEmpty());
    }
    assertFalse(descriptor.getAggregationLayouts().getEnabled());
    if (emptyLayouts) {
      assertTrue(descriptor.getAggregationLayouts().getLayoutList().isEmpty());
    }
  }

  private void setManualModeLayout(final AccelerationApiDescriptor descriptor) {
    // Now just add some dummy columns to see what happens if we modify the aggregation layout incorrectly.
    descriptor.getAggregationLayouts().setLayoutList(Lists.newArrayList(aggregationLayout));
    descriptor.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout));
    descriptor.setMode(AccelerationMode.MANUAL);
    descriptor.getAggregationLayouts().setEnabled(true);
  }

  private void setManualDummyModeLayout(final AccelerationApiDescriptor descriptor) {
    // Now just add some dummy columns to see what happens if we modify the aggregation layout incorrectly.
    descriptor.getAggregationLayouts().setLayoutList(Lists.newArrayList(dummyAggregationLayout));
    descriptor.getRawLayouts().setLayoutList(Lists.newArrayList(rawLayout));
    descriptor.setMode(AccelerationMode.MANUAL);
    descriptor.getAggregationLayouts().setEnabled(true);
  }

  private void checkManualModeLayout(final AccelerationApiDescriptor descriptor) {
    checkManualModeLayout(descriptor, true, false);
  }
  private void checkManualModeLayout(final AccelerationApiDescriptor descriptor, boolean isAggEnabled, boolean isRawEnabled) {
    assertEquals(isAggEnabled, descriptor.getAggregationLayouts().getEnabled());
    assertEquals(isRawEnabled, descriptor.getRawLayouts().getEnabled());
    assertEquals(AccelerationMode.MANUAL, descriptor.getMode());
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().size() == 1);
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getPartitionFieldList() == null
      || descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getPartitionFieldList().isEmpty());
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList() == null
      || descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList().isEmpty());
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getSortFieldList() == null
      || descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getSortFieldList().isEmpty());
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getDistributionFieldList() == null
      || descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getDistributionFieldList().isEmpty());
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getMeasureFieldList().equals(
      aggregationLayout.getDetails().getMeasureFieldList()));
    assertTrue(descriptor.getAggregationLayouts().getLayoutList().get(0).getDetails().getDimensionFieldList().equals(
      aggregationLayout.getDetails().getDimensionFieldList()));
    assertTrue(descriptor.getRawLayouts().getLayoutList().size() == 1
      && descriptor.getRawLayouts().getLayoutList().get(0).getDetails().getDisplayFieldList().equals(rawLayout.getDetails().getDisplayFieldList()));
  }

  private AccelerationApiDescriptor createAccelerationOnNewTable(String datasetFile) throws Exception {
    // retrieve json data - default to employee.json
    byte[] input;
    if (EMPLOYEES_WITH_NULL_FILE.equals(datasetFile)) {
      input = employeeWithNullJsonData;
    } else {
      input = employeeJsonData;
    }

    // create test file.
    Path path = getPopulator().getPath();
    File dataFile = File.createTempFile("testTable", ".json", path.toFile());
    dataFile.deleteOnExit();
    path = path.resolve(dataFile.getName());

    // write test file.
    try (FileOutputStream fos = new FileOutputStream(path.toAbsolutePath().toString())) {
      fos.write(input);
    }

    // accelerate the test file
    final List<String> fullPath = ImmutableList.<String>builder().add("dacfs").addAll(PathUtils.toPathComponents(path.toString())).build();
    DatasetConfig config = new DatasetConfig()
      .setName(dataFile.getName())
      .setFullPathList(fullPath)
      .setOwner(SYSTEM_USERNAME)
      .setCreatedAt(System.currentTimeMillis())
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER)
      .setPhysicalDataset(
        new PhysicalDataset()
          .setFormatSettings(new FileConfig().setType(FileType.JSON))
      );

    final NamespaceKey key = new NamespaceKey(fullPath);

    // create dataset info.
    l(CatalogService.class).createOrUpdateDataset(newNamespaceService(), new NamespaceKey("dacfs"), key, config);

    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(new DatasetPath(fullPath));
    return newApiDescriptor;
  }

  /**
   * Test pipeline factory
   */
  public static class TestPipelineFactory extends PipelineFactory {
    @Override
    public Pipeline newCreationPipeline(final Acceleration acceleration) {
      return wrapWithTestStages(super.newCreationPipeline(acceleration));
    }

    public Pipeline newAnalysis(final Acceleration acceleration) {
      return wrapWithTestStages(super.newAnalysis(acceleration));
    }

    @Override
    public Pipeline newUpdatePipeline(final Acceleration acceleration, final AccelerationDescriptor target) {
      return wrapWithTestStages(super.newUpdatePipeline(acceleration, target));
    }

    private Pipeline wrapWithTestStages(Pipeline p) {
      final List<Stage> newStages = Lists.newArrayList();
      for (Stage stage : p.getStages()) {
        newStages.add(new TestStage(stage));
      }
      return Pipeline.of(p.getAccelerationClone(), newStages);
    }
  }

  /**
   * Test stage that can fail
   */
  public static class TestStage implements Stage {

    private final Stage inner;

    public TestStage(Stage inner) {
      this.inner = Preconditions.checkNotNull(inner);
    }

    @Override
    public void execute(final StageContext context) {
      if (inner instanceof AnalysisStage) {
        if (failAnalysisStage) {
          throw new StageException("Test Analysis Failure Message", new RuntimeException("Test Analysis Failure"));
        } else {
          inner.execute(context);
        }
      } else if (inner instanceof SuggestionStage) {
        if (failSuggestionStage) {
          throw new StageException("Test Suggestion Failure Message", new RuntimeException("Test Suggestion Failure"));
        } else {
          inner.execute(context);
        }
      } else if (inner instanceof LayoutUpdateStage) {
        if (failUpdateLayoutStage) {
          throw new StageException("Test UpdateLayout Failure Message", new RuntimeException("Test UPdateLayout Failure"));
        } else {
          inner.execute(context);
        }
      } else if (inner instanceof BasicLayoutUpdateStage) {
        if (failBasicLayoutUpdateStage) {
          throw new StageException("Test BasicLayoutUpdate Failure Message", new RuntimeException("Test BasicLayoutUpdate Failure"));
        } else {
          inner.execute(context);
        }
      } else if (inner instanceof PlanningStage) {
        if (failPlanningStage) {
          throw new StageException("Test Planning Failure Message", new RuntimeException("Test Planning Failure"));
        } else {
          inner.execute(context);
        }
      } else if (inner instanceof ActivationStage) {
        if (failActivationStage) {
          throw new StageException("Test Activation Failure Message", new RuntimeException("Test Activation Failure"));
        } else {
          inner.execute(context);
        }
      } else {
        assertTrue("Unknown Stage, " + inner.getClass().getSimpleName(), false);
      }
    }
  }
}
