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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.service.accelerator.AccelerationTestUtil;

/**
 * Tests to make sure that acceleration refers to the correct dataset path
 * when the dataset is moved or renamed
 *
 */
public class TestDatasetChange extends AccelerationTestUtil {

  @Before
  public void setup() throws Exception {
    addCPSource(true);
    addEmployeesJson();
  }

  @After
  public void clear() throws Exception {
    getAccelerationService().developerService().clearAllAccelerations();
    deleteSource();
    deleteSpace();
  }

  /**
   * Rename a dataset and make sure that the acceleration has the new name
   * @throws Exception
   */
  @Test
  public void testDatasetRename() throws Exception {
    //create acceleration
    AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //check the dataset path associated with the acceleration
    assertEquals(newApiDescriptor.getContext().getDataset().getPathList(), EMPLOYEES_VIRTUAL.toPathList());

    //rename the dataset
    DatasetPath dsNew = new DatasetPath(Arrays.asList(TEST_SPACE, RENAMED_EMPLOYEES_VIRTUAL_DATASET_NAME));
    renameDataset(EMPLOYEES_VIRTUAL, dsNew);

    // reload the acceleration
    newApiDescriptor = pollAcceleration(newApiDescriptor.getId());

    //make sure that the dataset path associated with the acceleration points to the new new name
    assertEquals(newApiDescriptor.getContext().getDataset().getPathList(), dsNew.toPathList());

    //rename the dataset so that it can cleaned up
    renameDataset(dsNew, EMPLOYEES_VIRTUAL);
  }

  /**
   * Move a dataset and make sure that the acceleration has the new path
   * @throws Exception
   */
  @Test
  public void testDatasetMove() throws Exception {
    //create acceleration
    AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //check the dataset path associated with the acceleration
    assertEquals(newApiDescriptor.getContext().getDataset().getPathList(), EMPLOYEES_VIRTUAL.toPathList());

    //rename the dataset
    DatasetPath dsNew = new DatasetPath(Arrays.asList(TEST_SPACE, TEST_FOLDER, EMPLOYEES_VIRTUAL_DATASET_NAME));
    renameDataset(EMPLOYEES_VIRTUAL, dsNew);

    // reload the acceleration
    newApiDescriptor = pollAcceleration(newApiDescriptor.getId());

    //make sure that the dataset path associated with the acceleration points to the new new name
    assertEquals(newApiDescriptor.getContext().getDataset().getPathList(), dsNew.toPathList());

    //rename the dataset so that it can cleaned up
    renameDataset(dsNew, EMPLOYEES_VIRTUAL);
  }
}
