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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.service.accelerator.AccelerationTestUtil;
import com.dremio.service.accelerator.proto.SystemSettings;

/**
 * Test class to test acceleration deletions when dataset/folder/space/source are removed
 */
public class TestOrphanedAcceleration extends AccelerationTestUtil {

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

  private void setOrphanCleanupFrequency(long period) {
    SystemSettings settings = getAccelerationService().getSettings();
    settings.setOrphanCleanupInterval(period);
    getAccelerationService().configure(settings);
  }

  /**
   * Delete a dataset and make sure that the acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testDataSetDeletion() throws Exception {
    //create acceleration
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete dataset and wait for the cleanuptask to run
    deleteDataset(EMPLOYEES_VIRTUAL);
    setOrphanCleanupFrequency(10L);
    Thread.sleep(20L);

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Delete a Source and make sure that the underlying acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testSourceDeletion() throws Exception {
    //create acceleration
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete source and wait for the cleanuptask to run
    deleteSource();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Delete a Source and make sure that only the underlying acceleration is removed by the cleanup task
   * Acceleration not under the source should not removed
   * @throws Exception
   */
  @Test
  public void testSourceDeletionWithMutipleAccelerations() throws Exception {
    //create acceleration
    final AccelerationApiDescriptor newApiDescriptor1 = createAcceleration(EMPLOYEES);
    final AccelerationApiDescriptor newApiDescriptor2 = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor1.getId()).isPresent());
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor2.getId()).isPresent());

    //delete source and wait for the cleanuptask to run
    deleteSource();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor1.getId()).isPresent());

    //make sure that acceleration not under source is NOT removed
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor2.getId()).isPresent());
  }

  /**
   * Delete a space and make sure that the underlying acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testSpaceDeletion() throws Exception {
    //create acceleration
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete space and wait for the cleanuptask to run
    deleteSpace();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Delete a Source and make sure that only the underlying acceleration is removed by the cleanup task
   * Acceleration not under the source should not removed
   * @throws Exception
   */
  @Test
  public void testSspaceDeletionWithutipleAccelerations() throws Exception {
    //create acceleration
    final AccelerationApiDescriptor newApiDescriptor1 = createAcceleration(EMPLOYEES);
    final AccelerationApiDescriptor newApiDescriptor2 = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor1.getId()).isPresent());
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor2.getId()).isPresent());

    //delete source and wait for the cleanuptask to run
    deleteSpace();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that acceleration not under source is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor2.getId()).isPresent());

    //make sure that the acceleration is NOT removed
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor1.getId()).isPresent());
  }

  /**
   * Delete a folder and make sure that the underlying acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testFolderDeletion() throws Exception {
    //move dataset under folder
    moveDataset(EMPLOYEES_VIRTUAL, EMPLOYEES_UNDER_FOLDER);

    //create acceleration under folder
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_UNDER_FOLDER);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete folder and wait for the cleanuptask to run
    deleteFolder();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Create acceleration on a dataset under a space.
   * Now move it to a folder  under the source
   * Delete the folder and make sure that the underlying acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testDeleteFolderWithDatasetMovedIn() throws Exception {
    //create acceleration on dataset under space
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //move dataset under folder
    moveDataset(EMPLOYEES_VIRTUAL, EMPLOYEES_UNDER_FOLDER);

    //check if acceleration is still present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete folder and wait for the cleanuptask to run
    deleteFolder();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Create acceleration on a dataset under a folder.
   * Delete the parent space and make sure that the underlying acceleration is removed by the cleanup task
   * @throws Exception
   */
  @Test
  public void testSpaceDeletionWithDatasetMovedUnderFolder() throws Exception {
    //move dataset under folder
    moveDataset(EMPLOYEES_VIRTUAL, EMPLOYEES_UNDER_FOLDER);

    //create acceleration under folder
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_UNDER_FOLDER);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete space and wait for the cleanuptask to run
    deleteSpace();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

  /**
   * Create acceleration on a dataset under a folder.
   * Move it out of the folder to the parent space
   * Delete the folder and make sure that the underlying acceleration is NOT removed by the cleanup task.
   * Delete space and make sure that the acceleration is removed by the cleanup task.
   * @throws Exception
   */
  @Test
  public void testFolderDeletionAfterMovingOutDataset() throws Exception {
    //move dataset under folder
    moveDataset(EMPLOYEES_VIRTUAL, EMPLOYEES_UNDER_FOLDER);

    //create acceleration under folder
    final AccelerationApiDescriptor newApiDescriptor = createAcceleration(EMPLOYEES_UNDER_FOLDER);

    //move dataset out of folder
    moveDataset(EMPLOYEES_UNDER_FOLDER, EMPLOYEES_VIRTUAL);

    //check if acceleration is present
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //delete space and wait for the cleanuptask to run
    deleteFolder();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is NOT removed
    assertTrue(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());

    //now delete space
    //delete space and wait for the cleanuptask to run
    deleteSpace();
    getAccelerationService().developerService().removeOrphanedAccelerations();

    //make sure that the acceleration is removed
    assertFalse(getAccelerationService().getAccelerationById(newApiDescriptor.getId()).isPresent());
  }

}
