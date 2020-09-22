/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.dac.service.support;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.dac.proto.model.source.UpgradeStatus;
import com.dremio.dac.proto.model.source.UpgradeTaskRun;
import com.dremio.dac.proto.model.source.UpgradeTaskStore;
import com.dremio.dac.support.UpgradeStore;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.test.DremioTest;

/**
 * To test KVStore for Upgrade
 */
public class TestUpgradeStore {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final LegacyKVStoreProvider kvstore =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);

  private static UpgradeStore upgradeStore;

  @BeforeClass
  public static void beforeClass() throws Exception {
    kvstore.start();
    upgradeStore = new UpgradeStore(kvstore);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    kvstore.close();
  }

  @After
  public void afterTest() throws Exception {
    List<UpgradeTaskStore> upgradeTaskStoreList = upgradeStore.getAllUpgradeTasks();
    for (UpgradeTaskStore task : upgradeTaskStoreList) {
      upgradeStore.deleteUpgradeTaskStoreEntry(task.getId().getId());
    }
  }

  @Test
  public void testCreateAndUpdateUpgradeStoreTask() throws Exception {
    String upgradeTaskName = "UpgradeTask1";
    String upgradeTaskID = "UpgradeTask1_ID";
    long startTime = System.currentTimeMillis();
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.RUNNING)
      .setStartTime(startTime);
    UpgradeTaskStore upgradeTaskStore =
      upgradeStore.createUpgradeTaskStoreEntry(upgradeTaskID, upgradeTaskName, upgradeTaskRun);

    assertNotNull(upgradeTaskStore);
    assertEquals(upgradeTaskID, upgradeTaskStore.getId().getId());
    assertEquals(upgradeTaskName, upgradeTaskStore.getName());
    assertEquals(1, upgradeTaskStore.getRunsList().size());

    UpgradeTaskRun createdUgradeTaskRun = upgradeTaskStore.getRunsList().get(0);
    assertEquals(UpgradeStatus.RUNNING, createdUgradeTaskRun.getStatus());
    assertNull(createdUgradeTaskRun.getEndTime());
    assertEquals(startTime, createdUgradeTaskRun.getStartTime().longValue());

    assertFalse(upgradeStore.isUpgradeTaskCompleted(upgradeTaskID));

    long endTime = System.currentTimeMillis();
    createdUgradeTaskRun.setEndTime(endTime);
    createdUgradeTaskRun.setStatus(UpgradeStatus.COMPLETED);

    UpgradeTaskStore upgradeTaskStoreCompleted =
      upgradeStore.updateLastUpgradeRun(upgradeTaskID, upgradeTaskName, createdUgradeTaskRun);

    assertEquals(1, upgradeTaskStoreCompleted.getRunsList().size());

    UpgradeTaskRun createdUgradeTaskRunCompl = upgradeTaskStoreCompleted.getRunsList().get(0);
    assertEquals(UpgradeStatus.COMPLETED, createdUgradeTaskRunCompl.getStatus());
    assertEquals(endTime, createdUgradeTaskRunCompl.getEndTime().longValue());
    assertEquals(startTime, createdUgradeTaskRunCompl.getStartTime().longValue());

    assertTrue(upgradeStore.isUpgradeTaskCompleted(upgradeTaskID));
  }

  @Test
  public void testAddUpgradeRun() throws Exception {
    String upgradeTaskName = "UpgradeTask1";
    String upgradeTaskID = "UpgradeTask1_ID";
    long startTime = System.currentTimeMillis();
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.RUNNING)
      .setStartTime(startTime);

    upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun);

    UpgradeTaskRun upgradeTaskRun2 = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.COMPLETED)
      .setStartTime(startTime)
      .setEndTime(startTime);

    UpgradeTaskStore upgradeTaskStore =
      upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun2);

    assertEquals(2, upgradeTaskStore.getRunsList().size());

    UpgradeTaskRun createdUgradeTaskRunCompl = upgradeTaskStore.getRunsList().get(0);
    assertEquals(UpgradeStatus.RUNNING, createdUgradeTaskRunCompl.getStatus());
    assertEquals(startTime, createdUgradeTaskRunCompl.getStartTime().longValue());
    assertNull(createdUgradeTaskRunCompl.getEndTime());

    createdUgradeTaskRunCompl = upgradeTaskStore.getRunsList().get(1);
    assertEquals(UpgradeStatus.COMPLETED, createdUgradeTaskRunCompl.getStatus());
    assertEquals(startTime, createdUgradeTaskRunCompl.getEndTime().longValue());
    assertEquals(startTime, createdUgradeTaskRunCompl.getStartTime().longValue());

    assertTrue(upgradeStore.isUpgradeTaskCompleted(upgradeTaskID));
  }

  @Test
  public void testAddTerminalStateWOTime() throws Exception {
    String upgradeTaskName = "UpgradeTask1";
    String upgradeTaskID = "UpgradeTask1_ID";
    long startTime = System.currentTimeMillis();
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.RUNNING)
      .setStartTime(startTime);

    upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun);

    UpgradeTaskRun upgradeTaskRun2 = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.COMPLETED)
      .setEndTime(startTime);

    thrown.expect(IllegalStateException.class);
    upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun2);

    upgradeTaskRun2 = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.COMPLETED)
      .setStartTime(startTime);

    thrown.expect(IllegalStateException.class);
    upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun2);
  }

  @Test
  public void updateRunInTermState() throws Exception {
    String upgradeTaskName = "UpgradeTask1";
    String upgradeTaskID = "UpgradeTask1_ID";
    long startTime = System.currentTimeMillis();
    UpgradeTaskRun upgradeTaskRun = new UpgradeTaskRun()
      .setStatus(UpgradeStatus.FAILED)
      .setStartTime(startTime)
      .setEndTime(startTime);

    UpgradeTaskStore upgradeTaskStore = upgradeStore.addUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun);
    UpgradeTaskRun upgradeTaskRun1 = upgradeTaskStore.getRunsList().get(0);

    upgradeTaskRun1.setStatus(UpgradeStatus.COMPLETED);

    thrown.expect(IllegalStateException.class);
    upgradeStore.updateLastUpgradeRun(upgradeTaskID, upgradeTaskName, upgradeTaskRun1);
  }
}
