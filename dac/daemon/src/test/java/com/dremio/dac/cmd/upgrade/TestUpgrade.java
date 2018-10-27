/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.dremio.dac.server.DACConfig;
import com.dremio.test.DremioTest;

/**
 * Test for {@code Upgrade}
 */
public class TestUpgrade extends DremioTest {
  /**
   * A test upgrade task
   */
  public static final class TopPriorityTask extends UpgradeTask {
    public TopPriorityTask() {
      super("test-top-priority-class", UpgradeTask.VERSION_106, UpgradeTask.VERSION_212, Integer.MIN_VALUE);
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  /**
   * A test upgrade task
   */
  public static final class LowPriorityTask extends UpgradeTask {
    public LowPriorityTask() {
      super("test-low-priority-class", UpgradeTask.VERSION_106, UpgradeTask.VERSION_212, Integer.MAX_VALUE);
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  /**
   * Verify that we don't add a task whose version is higher than the current version,
   * as it would create a loop where user would have to upgrade but would never be able
   * to update the version to a greater version than the task in the kvstore.
   */
  @Test
  public void testMaxTaskVersion() {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    // Making sure that current version is newer that all upgrade tasks
    assertTrue(
        String.format("One task has a newer version (%s) than the current server version (%s)", upgrade.getTasksGreatestMaxVersion().get(), VERSION),
        Upgrade.UPGRADE_VERSION_ORDERING.compare(upgrade.getTasksGreatestMaxVersion().get(), VERSION) <= 0);
  }


  /**
   * Verify that tasks are discovered properly and are correctly ordered
   */
  @Test
  public void testTasksOrder() {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    List<? extends UpgradeTask> tasks = upgrade.getUpgradeTasks();
    // Hamcrest Matchers#contains(...) guarantee both order and size!
    assertThat(tasks, contains(
        // Test task
        instanceOf(TopPriorityTask.class),
        // Production tasks
        instanceOf(DatasetConfigUpgrade.class),
        instanceOf(ReIndexAllStores.class),
        instanceOf(EnableLegacyDialectForBelowV3.class),
        instanceOf(UpdateDatasetSplitIdTask.class),
        instanceOf(UpdateS3CredentialType.class),
        instanceOf(MigrateAccelerationMeasures.class),
        instanceOf(SetDatasetExpiry.class),
        instanceOf(SetAccelerationRefreshGrace.class),
        instanceOf(MarkOldMaterializationsAsDeprecated.class),
        instanceOf(MoveFromAccelerationsToReflections.class),
        instanceOf(DeleteInternalSources.class),
        instanceOf(MoveFromAccelerationSettingsToReflectionSettings.class),
        instanceOf(ConvertJoinInfo.class),
        instanceOf(CompressHiveTableAttrs.class),
        instanceOf(DeleteHistoryOfRenamedDatasets.class),
        instanceOf(DeleteHive121BasedInputSplits.class),
        instanceOf(MinimizeJobResultsMetadata.class),
        instanceOf(UpdateExternalReflectionHash.class),
        instanceOf(DeleteSysTablesMetadata.class),
        // Final test task
        instanceOf(LowPriorityTask.class)));
  }

}
