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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationInfoApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDimensionFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutFieldApiDescriptor;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.google.common.collect.Lists;

/**
 * Test acceleration pipeline, create/modify layouts, etc.
 */
public class TestAccelerationPipeline extends AccelerationTestUtil {

  public static final String TEST_SOURCE = "src";

  private static final String badCsvFile = "namesNonUtf8.csv";
  private static final String okayCsvFile = "names.csv";
  private static final DatasetPath badNamesDataSet = new DatasetPath(Arrays.asList(TEST_SOURCE, badCsvFile));
  private static final DatasetPath okayNamesDataSet = new DatasetPath(Arrays.asList(TEST_SOURCE, okayCsvFile));

  private static AtomicBoolean created = new AtomicBoolean(false);

  @Before
  public void setup() throws Exception {
    if (created.getAndSet(true)) {
      return;
    }

    addCPSource();
    CatalogService catalog = l(CatalogService.class);
    createPhysicalDatasetSourceFile(badCsvFile, badNamesDataSet, catalog);
    createPhysicalDatasetSourceFile(okayCsvFile, okayNamesDataSet, catalog);
  }

  @After
  public void clear() throws Exception {
    getAccelerationService().developerService().clearAllAccelerations();
  }

  private void createPhysicalDatasetSourceFile(String name, DatasetPath datasetPath, CatalogService catalog) throws NamespaceException {
    final TextFileConfig textFileConfig = new TextFileConfig()
      .setAutoGenerateColumnNames(true)
      .setComment("#")
      .setEscape("`")
      .setExtractHeader(true)
      .setFieldDelimiter(",")
      .setLineDelimiter("\n")
      .setQuote("\"")
      .setSkipFirstLine(false);
    textFileConfig.setOwner(DEFAULT_USERNAME);
    textFileConfig.setCtime(System.currentTimeMillis());
    textFileConfig.setFullPath(datasetPath.toPathList());
    textFileConfig.setName(name);
    final DatasetConfig dataSet = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
      .setFullPathList(datasetPath.toPathList())
      .setName(name)
      .setCreatedAt(System.currentTimeMillis())
      .setVersion(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(textFileConfig.asFileConfig()));
    catalog.createOrUpdateDataset(newNamespaceService(), new NamespaceKey(datasetPath.toNamespaceKey().getPathComponents().subList(0, 1)), datasetPath.toNamespaceKey(), dataSet);
  }

  @Ignore("DX-6010")
  @Test
  public void testOkayNameAddToMeasureBasicMode() throws Exception {
    final String name = "lower";
    testSaveAndMaterializeAfterModify(okayNamesDataSet, name, true, true);
  }

  @Ignore("DX-6010")
  @Test
  public void testOkayNameAddToDimensionBasicMode() throws Exception {
    final String name = "upper";
    testSaveAndMaterializeAfterModify(okayNamesDataSet, name, false, true);
  }

  @Ignore("DX-6010")
  @Test
  public void testBadNameAddToMeasureBasicMode() throws Exception {
    final String badName = "lowerï¿œ";
    testSaveAndMaterializeAfterModify(badNamesDataSet, badName, true, true);
  }

  @Ignore("DX-6010")
  @Test
  public void testBadNameAddToDimensionBasicMode() throws Exception {
    final String badName = "upperï¿œ";
    testSaveAndMaterializeAfterModify(badNamesDataSet, badName, false, true);
  }

  @Test
  public void testOkayNameAddToMeasureAdvMode() throws Exception {
    final String name = "lower";
    testSaveAndMaterializeAfterModify(okayNamesDataSet, name, true, false);
  }

  @Test
  public void testOkayNameAddToDimensionAdvMode() throws Exception {
    final String name = "upper";
    testSaveAndMaterializeAfterModify(okayNamesDataSet, name, false, false);
  }

  @Test
  public void testBadNameAddToMeasureAdvMode() throws Exception {
    final String badName = "lowerï¿œ";
    testSaveAndMaterializeAfterModify(badNamesDataSet, badName, true, false);
  }

  @Test
  public void testBadNameAddToDimensionAdvMode() throws Exception {
    final String badName = "upperï¿œ";
    testSaveAndMaterializeAfterModify(badNamesDataSet, badName, false, false);
  }

  @Test
  public void testSaveBasicModeWithoutEnable() throws Exception {
    testSaveWithoutEnable(false);
  }

  @Ignore("DX-6032")
  @Test
  public void testSaveBasicModeWithoutEnableWithDelay() throws Exception {
    testSaveWithoutEnable(true);
  }

  @Test
  public void testAccelerateChangeLayoutAccelerate() throws Exception {
    // Accelerate once.
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("upper");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField));

    polled.getAggregationLayouts().setEnabled(true);
    saveAcceleration(polled.getId(), polled);

    // Validate the first acceleration attempt.
    // Make sure that getting the acceleration information via acceleration id
    AccelerationApiDescriptor afterSave = pollAcceleration(polled.getId());
    assertTrue(afterSave.getContext().getLogicalAggregation().getMeasureList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertTrue(afterSave.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    assertFalse(afterSave.getAggregationLayouts().getEnabled() && afterSave.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    AccelerationInfoApiDescriptor accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
    afterSave = pollAcceleration(polled.getId());

    // Reverse the layout.
    afterSave.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(dimensionField));
    afterSave.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(measureField));

    // Validate the second acceleration attempt.
    AccelerationApiDescriptor saved = saveAcceleration(afterSave.getId(), afterSave);
    waitForMaterialization(saved.getId(), true);
    assertTrue(saved.getContext().getLogicalAggregation().getMeasureList().size() == 1
      && saved.getContext().getLogicalAggregation().getMeasureList().contains(dimensionField));
    assertTrue(saved.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && saved.getContext().getLogicalAggregation().getDimensionList().contains(measureField));
    assertFalse(saved.getAggregationLayouts().getEnabled() && saved.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
  }

  private void testSaveWithoutEnable(boolean sleep) throws Exception {
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("upper");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField));
    saveAcceleration(polled.getId(), polled);

    if (sleep) {
      Thread.sleep(3000);
    }

    // Make sure that getting the acceleration information via acceleration id
    final AccelerationApiDescriptor afterSave = pollAcceleration(polled.getId());
    assertTrue(afterSave.getContext().getLogicalAggregation().getMeasureList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertTrue(afterSave.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    assertFalse(afterSave.getAggregationLayouts().getEnabled() && afterSave.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    final AccelerationInfoApiDescriptor accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
  }

  @Test
  public void testAddSameColumnAsMeasureAndDimension() throws Exception {
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("lower");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField));
    saveAcceleration(polled.getId(), polled);

    // Make sure that getting the acceleration information via acceleration id
    final AccelerationApiDescriptor afterSave = pollAcceleration(polled.getId());
    // Check that measure field is omitted if it exists as a dimension.
    assertTrue(afterSave.getContext().getLogicalAggregation().getMeasureList().size() == 0
      && !afterSave.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertTrue(afterSave.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    assertFalse(afterSave.getAggregationLayouts().getEnabled() && afterSave.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    final AccelerationInfoApiDescriptor accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
  }

  @Test
  public void testDuplicateDimension() throws Exception {
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("upper");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField, dimensionField));
    saveAcceleration(polled.getId(), polled);

    // Wait for the update pipeline to be completed before running assertions on results.
    int timeoutCounter = 0;
    while (!getAccelerationService().isPipelineCompletedOrNotStarted(polled.getId())) {
      Thread.sleep(500);
      ++timeoutCounter;

      if (timeoutCounter > 10) {
        Assert.fail("Timed out waiting for pipeline to complete");
      }
    }

    // Make sure that getting the acceleration information via acceleration id
    final AccelerationApiDescriptor afterSave = pollAcceleration(polled.getId());

    assertTrue(afterSave.getContext().getLogicalAggregation().getMeasureList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    // Check that the duplicate dimension has been pruned out.
    assertTrue(afterSave.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    assertFalse(afterSave.getAggregationLayouts().getEnabled() && afterSave.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    final AccelerationInfoApiDescriptor accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
  }

  @Test
  public void testDuplicateMeasure() throws Exception {
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("upper");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField, measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField));
    saveAcceleration(polled.getId(), polled);

    // Make sure that getting the acceleration information via acceleration id
    final AccelerationApiDescriptor afterSave = pollAcceleration(polled.getId());
    // Check that the duplicate measure has been pruned out.
    assertTrue(afterSave.getContext().getLogicalAggregation().getMeasureList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertTrue(afterSave.getContext().getLogicalAggregation().getDimensionList().size() == 1
      && afterSave.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    assertFalse(afterSave.getAggregationLayouts().getEnabled() && afterSave.getRawLayouts().getEnabled());

    // Make sure that getting the acceleration information via dataset name works too
    final AccelerationInfoApiDescriptor accFromDataset = getAccelerationForDataset(okayNamesDataSet);
    assertFalse(accFromDataset.getAggregationEnabled() && accFromDataset.getRawAccelerationEnabled());
  }

  @Test
  public void testDoubleSave() throws Exception {
    final AccelerationApiDescriptor accID = createNewAcceleration(okayNamesDataSet);
    final LayoutFieldApiDescriptor measureField = new LayoutFieldApiDescriptor("lower");
    final LayoutFieldApiDescriptor dimensionField = new LayoutFieldApiDescriptor("upper");
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(accID.getId());
    assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(measureField));
    assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(dimensionField));
    polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(measureField));
    polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(dimensionField));
    final AccelerationApiDescriptor saved = saveAcceleration(polled.getId(), polled);
    final AccelerationApiDescriptor saved2 = saveAcceleration(saved.getId(), saved);

    assertTrue(saved2.getContext().getLogicalAggregation().getMeasureList().size() == 1);
    assertTrue(saved2.getContext().getLogicalAggregation().getDimensionList().size() == 1);
  }

  /**
   * Adds a column "name" to the AggregationLayouts (basic mode), if measure is set to true as a measure; otherwise as a dimension.
   */
  private void testSaveAndMaterializeAfterModify(DatasetPath path, String name, boolean measure, boolean basicMode) throws Exception {
    final AccelerationApiDescriptor newApiDescriptor = createNewAcceleration(path);
    final LayoutFieldApiDescriptor fieldDescriptor = new LayoutFieldApiDescriptor(name);
    final LayoutDimensionFieldApiDescriptor dimDescriptor = new LayoutDimensionFieldApiDescriptor(name).setGranularity(DimensionGranularity.NORMAL);
    final AccelerationApiDescriptor polled = waitForLayoutGeneration(newApiDescriptor.getId());
    // DX-6017
    // Should allow just dimensions in AggregationLayout, but it is not yet supported, so just add a dummy measure.
    final LayoutFieldApiDescriptor nameFieldDescriptor = new LayoutFieldApiDescriptor("name");

    // In the layout make sure that the given column does not exist in measure list and then add it to the logicalAggregation layout
    assertEquals(1, polled.getAggregationLayouts().getLayoutList().size());
    if (measure) {
      // Make the measure be just this one column
      assertFalse(polled.getAggregationLayouts().getLayoutList().get(0).getDetails().getMeasureFieldList().contains(fieldDescriptor));
      assertFalse(polled.getContext().getLogicalAggregation().getMeasureList().contains(fieldDescriptor));

      polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(fieldDescriptor));
      if (!basicMode) {
        // DX-6034 should not need this line above
        polled.getAggregationLayouts().getLayoutList().get(0).getDetails().setMeasureFieldList(Lists.newArrayList(fieldDescriptor));
      }
    } else {
      // Make the dimension be just this one column
      assertFalse(polled.getAggregationLayouts().getLayoutList().get(0).getDetails().getDimensionFieldList().contains(dimDescriptor));
      assertFalse(polled.getContext().getLogicalAggregation().getDimensionList().contains(fieldDescriptor));

      polled.getContext().getLogicalAggregation().setDimensionList(Lists.newArrayList(fieldDescriptor));
      polled.getContext().getLogicalAggregation().setMeasureList(Lists.newArrayList(nameFieldDescriptor));
      if (!basicMode) {
        // DX-6034 should not need the two lines above
        polled.getAggregationLayouts().getLayoutList().get(0).getDetails().setDimensionFieldList(Lists.newArrayList(dimDescriptor));
        polled.getAggregationLayouts().getLayoutList().get(0).getDetails().setMeasureFieldList(Lists.newArrayList(nameFieldDescriptor));
      }
    }
    polled.getAggregationLayouts().setEnabled(true);
    if (!basicMode) {
      polled.setMode(AccelerationMode.MANUAL);
    }

    final AccelerationApiDescriptor saved = saveAcceleration(polled.getId(), polled);
    waitForMaterialization(saved.getId(), true);

    // Now that we have materialized, let's get all the layouts and take a look
    final AccelerationApiDescriptor afterMaterialize = pollAcceleration(saved.getId());
    assertTrue(afterMaterialize.getAggregationLayouts().getEnabled());
    assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().size() == 1);
    if (measure) {
      assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getMeasureFieldList().contains(fieldDescriptor));
      if (basicMode) {
        assertTrue(afterMaterialize.getContext().getLogicalAggregation().getMeasureList().contains(fieldDescriptor));
      }
    } else {
      assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getDimensionFieldList().contains(dimDescriptor));
      assertTrue(afterMaterialize.getAggregationLayouts().getLayoutList().get(0).getDetails().getMeasureFieldList().contains(nameFieldDescriptor));
      if (basicMode) {
        assertTrue(afterMaterialize.getContext().getLogicalAggregation().getDimensionList().contains(fieldDescriptor));
        assertTrue(afterMaterialize.getContext().getLogicalAggregation().getMeasureList().contains(nameFieldDescriptor));
      }
    }
  }
}
