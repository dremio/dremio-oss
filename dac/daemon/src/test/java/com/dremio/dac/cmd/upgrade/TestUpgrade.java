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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.cmd.upgrade.LegacyUpgradeTask.VERSION_203;
import static com.dremio.dac.cmd.upgrade.LegacyUpgradeTask.VERSION_205;
import static com.dremio.dac.cmd.upgrade.Upgrade.UPGRADE_VERSION_ORDERING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.Version;
import com.dremio.dac.proto.model.source.UpgradeStatus;
import com.dremio.dac.proto.model.source.UpgradeTaskStore;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.support.SupportService;
import com.dremio.dac.support.UpgradeStore;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Test for {@code Upgrade}
 */
public class TestUpgrade extends DremioTest {
  /**
   * A test upgrade task
   */
  public static final class TopPriorityTask extends UpgradeTask {
    public TopPriorityTask() {
      super("test-top-priority-class", ImmutableList.of(SetExportType.taskUUID));
    }

    @Override
    public String getTaskUUID() {
      return "test-top-priority-class";
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
      super("test-low-priority-class", ImmutableList.of("test-top-priority-class"));
    }

    @Override
    public String getTaskUUID() {
      return "test-low-priority-class";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {

    }
  }

  private static final LocalKVStoreProvider kvStoreProvider = new LocalKVStoreProvider(
    CLASSPATH_SCAN_RESULT, null, true, false);

  private static LegacyKVStoreProvider legacyKVStoreProvider;

  private static UpgradeStore upgradeStore;

  @BeforeClass
  public static void beforeClass() throws Exception {
    kvStoreProvider.start();
    legacyKVStoreProvider = kvStoreProvider.asLegacy();
    upgradeStore = new UpgradeStore(legacyKVStoreProvider);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    legacyKVStoreProvider.close();
  }

  @After
  public void afterTest() throws Exception {
    List<UpgradeTaskStore> upgradeTaskStoreList = upgradeStore.getAllUpgradeTasks();
    for (UpgradeTaskStore task : upgradeTaskStoreList) {
      upgradeStore.deleteUpgradeTaskStoreEntry(task.getId().getId());
    }
  }

  @Test
  public void testTaskUpgrade() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_203);
    Version kvStoreVersion = VERSION_203;
    final UpgradeContext context = tasksExecutor(kvStoreVersion, ImmutableList.of(myTask));
    assertThat(((TestUpgradeFailORSuccessTask) myTask).isTaskRun).isTrue();

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTask.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), myTask.getTaskName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.COMPLETED, upgradeEntries.get(0).getRunsList().get(0).getStatus());
  }

  @Test
  public void testNoUpgradeWithMaxVersion() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_203);
    Version kvStoreVersion = VERSION_205;
    final UpgradeContext context = tasksExecutor(kvStoreVersion, ImmutableList.of(myTask));
    assertThat(((TestUpgradeFailORSuccessTask) myTask).isTaskRun).isFalse();

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTask.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.OUTDATED, upgradeEntries.get(0).getRunsList().get(0).getStatus());
  }

  @Test
  public void testTaskUpgradeFail() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Failed Task", VERSION_203);
    Version kvStoreVersion = VERSION_203;
    ((TestUpgradeFailORSuccessTask)myTask).toFail = true;

    assertThatThrownBy(() -> tasksExecutor(kvStoreVersion, ImmutableList.of(myTask)))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("taskFailure");

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTask.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.FAILED, upgradeEntries.get(0).getRunsList().get(0).getStatus());
  }

  @Test
  public void testUpgradeTaskCompleted() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_205);
    Version kvStoreVersion = VERSION_203;
    final UpgradeContext context = tasksExecutor(kvStoreVersion, ImmutableList.of(myTask));
    assertThat(((TestUpgradeFailORSuccessTask) myTask).isTaskRun).isTrue();

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTask.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.COMPLETED, upgradeEntries.get(0).getRunsList().get(0).getStatus());

    // try to upgrade again
    UpgradeTask myTaskAgain = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_205);

    tasksExecutor(kvStoreVersion, ImmutableList.of(myTask));
    assertThat(((TestUpgradeFailORSuccessTask) myTaskAgain).isTaskRun).isFalse();

    upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTaskAgain.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.COMPLETED, upgradeEntries.get(0).getRunsList().get(0).getStatus());
  }

  @Test
  public void testUpgradeAfterFailure() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Failed Task", VERSION_205);
    Version kvStoreVersion = VERSION_203;

    ((TestUpgradeFailORSuccessTask)myTask).toFail = true;

    assertThatThrownBy(() -> tasksExecutor(kvStoreVersion, ImmutableList.of(myTask)))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("taskFailure");

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTask.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(1, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.FAILED, upgradeEntries.get(0).getRunsList().get(0).getStatus());

    // try to upgrade again
    UpgradeTask myTaskAgain = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_205);
    ((TestUpgradeFailORSuccessTask) myTaskAgain).toFail = false;

    assertThatThrownBy(() -> tasksExecutor(kvStoreVersion, ImmutableList.of(myTask)))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("taskFailure");

    upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(1, upgradeEntries.size());
    assertEquals(myTaskAgain.getTaskUUID(), upgradeEntries.get(0).getId().getId());
    assertEquals(TestUpgradeFailORSuccessTask.class.getSimpleName(), upgradeEntries.get(0).getName());
    assertEquals(2, upgradeEntries.get(0).getRunsList().size());
    assertEquals(UpgradeStatus.FAILED, upgradeEntries.get(0).getRunsList().get(0).getStatus());
    assertEquals(UpgradeStatus.FAILED, upgradeEntries.get(0).getRunsList().get(1).getStatus());
  }

  @Test
  public void testMultipleTasks() throws Exception {
    UpgradeTask myTask = new TestUpgradeFailORSuccessTask("Test Upgrade Task", VERSION_205);
    UpgradeTask myTask1 = new TestUpgradeTask("Test Upgrade2 Task", VERSION_203);
    Version kvStoreVersion = VERSION_203;
    final UpgradeContext context = tasksExecutor(kvStoreVersion, ImmutableList.of(myTask, myTask1));
    assertThat(((TestUpgradeFailORSuccessTask) myTask).isTaskRun).isTrue();
    assertThat(((TestUpgradeTask) myTask1).isTaskRun).isTrue();

    List<UpgradeTaskStore> upgradeEntries = upgradeStore.getAllUpgradeTasks();
    assertEquals(2, upgradeEntries.size());
    Set<String> expectedNames = ImmutableSet.of(TestUpgradeTask.class.getSimpleName(),
      TestUpgradeFailORSuccessTask.class.getSimpleName());

    Set<String> realNames = Sets.newHashSet();
    for (UpgradeTaskStore task : upgradeEntries) {
      assertEquals(1, task.getRunsList().size());
      assertEquals(UpgradeStatus.COMPLETED, task.getRunsList().get(0).getStatus());
      realNames.add(task.getName());
    }
    assertEquals(expectedNames, realNames);
  }

  private UpgradeContext tasksExecutor(Version kvStoreVersion, List<UpgradeTask> tasks) throws Exception {
    final UpgradeContext context = new UpgradeContext(kvStoreProvider, legacyKVStoreProvider, null, null, null);
    List<UpgradeTask> tasksToRun = new ArrayList<>();
    for(UpgradeTask task: tasks) {
      if (upgradeStore.isUpgradeTaskCompleted(task.getTaskUUID())) {
        continue;
      }
      tasksToRun.add(task);
    }

    if (!tasksToRun.isEmpty()) {
      for (UpgradeTask task : tasksToRun) {
        Upgrade.upgradeExternal(task, context, upgradeStore, kvStoreVersion);
      }
    }
    return context;
  }

  private static class TestUpgradeFailORSuccessTask extends UpgradeTask implements LegacyUpgradeTask {

    private boolean toFail = false;
    private boolean isTaskRun = false;
    private Version maxVersion;

    public TestUpgradeFailORSuccessTask(String name, Version maxVersion) {
      super(name, ImmutableList.of());
      this.maxVersion = maxVersion;
    }

    @Override
    public Version getMaxVersion() {
      return maxVersion;
    }

    @Override
    public String getTaskUUID() {
      return "TestUpgradeFailORSuccessTask_ID";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {
      isTaskRun = true;
      if (toFail) {
        throw new RuntimeException("taskFailure");
      }
    }
  }

  private static class TestUpgradeTask extends UpgradeTask implements LegacyUpgradeTask {

    private boolean toFail = false;
    private boolean isTaskRun = false;
    private Version maxVersion;

    public TestUpgradeTask(String name, Version maxVersion) {
      super(name, ImmutableList.of());
      this.maxVersion = maxVersion;
    }

    @Override
    public Version getMaxVersion() {
      return maxVersion;
    }

    @Override
    public String getTaskUUID() {
      return "TestUpgradeTask_ID";
    }

    @Override
    public void upgrade(UpgradeContext context) throws Exception {
      isTaskRun = true;
      if (toFail) {
        throw new RuntimeException("taskFailure");
      }
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
    final Optional<Version> tasksGreatestMaxVersion = upgrade.getUpgradeTasks().stream()
      .filter((v) -> v instanceof LegacyUpgradeTask)
      .map((v) -> ((LegacyUpgradeTask)v).getMaxVersion() )
      .max(UPGRADE_VERSION_ORDERING);

    // Making sure that current version is newer that all upgrade tasks
    assertThat(UPGRADE_VERSION_ORDERING.compare(tasksGreatestMaxVersion.get(), VERSION) <= 0)
      .as(String.format("One task has a newer version (%s) than the current server version (%s)", tasksGreatestMaxVersion.get(), VERSION))
      .isTrue();
  }


  /**
   * Verify that tasks are discovered properly and are correctly ordered
   */
  @Test
  public void testTasksOrder() {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    List<? extends UpgradeTask> tasks = upgrade.getUpgradeTasks();

    // Get MapR profile variable from Maven surfire plugin
    boolean isMapr = Boolean.valueOf(System.getProperty("dremio.mapr.profile"));

    // Dremio build contains S3 plugin, UpdateS3CredentialType is included in the list of
    // upgrade tasks. The Dremio MapR distribution does not include S3 plugin, therefore UpdateS3CredentialType
    // should not be included in the list of upgrade tasks. UpdateDatasetSplitIdTask has a child,
    // UpdateS3CredentialType, which should not be included as an upgrade task in the MapR distribution.
    // We want to first determine whether UpdateDatasetSplitIdTask and UpdateS3CredentialType are in the list
    // of tasks, then test accordingly.
    boolean containsS3Task = false;
    int s3TaskIndex = 0;
    boolean containsDatasetSplitTask = false;
    int datasetSplitTaskIndex = 0;

    for (int i = 0; i < tasks.size(); i++) {
      String taskName = tasks.get(i).getClass().getName();
      if ("com.dremio.dac.cmd.upgrade.UpdateS3CredentialType".equals(taskName)) {
        containsS3Task = true;
        s3TaskIndex = i;
      } else if ("com.dremio.dac.cmd.upgrade.UpdateDatasetSplitIdTask".equals(taskName)) {
        containsDatasetSplitTask = true;
        datasetSplitTaskIndex = i;
      }
    }

    if (isMapr) {
      assertThat(containsS3Task).isFalse();
    } else {
      // Following conditions must be satisfied to ensure correct upgrade tasks ordering.
      // 1. All three tasks are in the list of tasks
      assertThat(containsDatasetSplitTask).isTrue();
      assertThat(containsS3Task).isTrue();
      // 2. Both child tasks are successive of UpdateDatasetSplitIdTask
      assertThat(s3TaskIndex).isGreaterThan(datasetSplitTaskIndex);
      // Remove UpdateS3CredentialType from the list, so vanilla and MapR distribution can be
      // tested with the same upgrade task list
      tasks.remove(s3TaskIndex);
    }

    // WHEN creating new UpgradeTask - please add it to the list
    // in order to get taskUUID you can run
    // testNoDuplicateUUID() test - it will generate one
    // tasks will not include TestUpgradeFailORSuccessTask and TestUpgradeTask
    // because they don't have default ctor
    assertThat(tasks).hasSize(12);
    assertThat(tasks.get(0)).isInstanceOf(ReIndexAllStores.class);
    assertThat(tasks.get(1)).isInstanceOf(UpdateDatasetSplitIdTask.class);
    assertThat(tasks.get(2)).isInstanceOf(DeleteHistoryOfRenamedDatasets.class);
    assertThat(tasks.get(3)).isInstanceOf(DeleteHive121BasedInputSplits.class);
    assertThat(tasks.get(4)).isInstanceOf(MinimizeJobResultsMetadata.class);
    assertThat(tasks.get(5)).isInstanceOf(UpdateExternalReflectionHash.class);
    assertThat(tasks.get(6)).isInstanceOf(DeleteSysMaterializationsMetadata.class);
    assertThat(tasks.get(7)).isInstanceOf(SetTableauDefaults.class);
    assertThat(tasks.get(8)).isInstanceOf(SetExportType.class);
    assertThat(tasks.get(9)).isInstanceOf(TopPriorityTask.class);
    assertThat(tasks.get(10)).isInstanceOf(LowPriorityTask.class);
    assertThat(tasks.get(11)).isInstanceOf(DeleteSnowflakeCommunitySource.class);
  }

  @Test
  public void testTasksWithoutUUID() throws Exception {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    List<? extends UpgradeTask> tasks = upgrade.getUpgradeTasks();
    tasks.forEach(task -> assertNotNull(
      String.format(
        "Need to add UUID to task: '%s'. For example: %s", task.getTaskName(), UUID.randomUUID().toString()),
      task.getTaskUUID()));
  }

  @Test
  public void testNoDuplicateUUID() {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    List<? extends UpgradeTask> tasks = upgrade.getUpgradeTasks();
    Set<String> uuidToCount = new HashSet<>();
    assertThat(tasks).allSatisfy(task -> assertThat(uuidToCount.add(task.getTaskUUID())).isTrue());
  }

  @Test
  public void testDependenciesResolver() throws Exception {
    DACConfig dacConfig = DACConfig.newConfig();
    Upgrade upgrade = new Upgrade(dacConfig, CLASSPATH_SCAN_RESULT, false);

    List<? extends UpgradeTask> tasks = upgrade.getUpgradeTasks();

    // Get MapR profile variable from Maven surfire plugin
    boolean isMapr = Boolean.valueOf(System.getProperty("dremio.mapr.profile"));

    // Dremio build contains S3 plugin, UpdateS3CredentialType is included in the list of
    // upgrade tasks. The Dremio MapR distribution does not include S3 plugin, therefore UpdateS3CredentialType
    // should not be included in the list of upgrade tasks. UpdateDatasetSplitIdTask has a child,
    // UpdateS3CredentialType, which should not be included as an upgrade task in the MapR distribution.
    // We want to first determine whether UpdateDatasetSplitIdTask and UpdateS3CredentialType are in the list
    // of tasks, then test accordingly.
    boolean containsS3Task = false;
    int s3TaskIndex = 0;
    boolean containsDatasetSplitTask = false;
    int datasetSplitTaskIndex = 0;

    for (int i = 0; i < tasks.size(); i++) {
      String taskName = tasks.get(i).getClass().getName();
      if ("com.dremio.dac.cmd.upgrade.UpdateS3CredentialType".equals(taskName)) {
        containsS3Task = true;
        s3TaskIndex = i;
      } else if ("com.dremio.dac.cmd.upgrade.UpdateDatasetSplitIdTask".equals(taskName)) {
        containsDatasetSplitTask = true;
        datasetSplitTaskIndex = i;
      }
    }

    if (isMapr) {
      assertThat(containsS3Task).isFalse();
    } else {
      // Following conditions must be satisfied to ensure correct upgrade tasks ordering.
      // 1. All three tasks are in the list of tasks
      assertThat(containsDatasetSplitTask).isTrue();
      assertThat(containsS3Task).isTrue();
      // 2. Both child tasks are successive of UpdateDatasetSplitIdTask
      assertThat(s3TaskIndex).isGreaterThan(datasetSplitTaskIndex);
      // Remove UpdateS3CredentialType from the list, so vanilla and MapR distribution can be
      // tested with the same upgrade task list
      tasks.remove(s3TaskIndex);
    }

    Collections.shuffle(tasks);
    UpgradeTaskDependencyResolver upgradeTaskDependencyResolver = new UpgradeTaskDependencyResolver(tasks);
    List<UpgradeTask> resolvedTasks = upgradeTaskDependencyResolver.topologicalTasksSort();

    // WHEN creating new UpgradeTask - please add it to the list
    // in order to get taskUUID you can run
    // testNoDuplicateUUID() test - it will generate one
    // tasks will not include TestUpgradeFailORSuccessTask and TestUpgradeTask
    // because they don't have default ctor
    assertThat(resolvedTasks).hasSize(12);
    assertThat(resolvedTasks.get(0)).isInstanceOf(ReIndexAllStores.class);
    assertThat(resolvedTasks.get(1)).isInstanceOf(UpdateDatasetSplitIdTask.class);
    assertThat(resolvedTasks.get(2)).isInstanceOf(DeleteHistoryOfRenamedDatasets.class);
    assertThat(resolvedTasks.get(3)).isInstanceOf(DeleteHive121BasedInputSplits.class);
    assertThat(resolvedTasks.get(4)).isInstanceOf(MinimizeJobResultsMetadata.class);
    assertThat(resolvedTasks.get(5)).isInstanceOf(UpdateExternalReflectionHash.class);
    assertThat(resolvedTasks.get(6)).isInstanceOf(DeleteSysMaterializationsMetadata.class);
    assertThat(resolvedTasks.get(7)).isInstanceOf(SetTableauDefaults.class);
    assertThat(resolvedTasks.get(8)).isInstanceOf(SetExportType.class);
    assertThat(resolvedTasks.get(9)).isInstanceOf(TopPriorityTask.class);
    assertThat(resolvedTasks.get(10)).isInstanceOf(LowPriorityTask.class);
    assertThat(resolvedTasks.get(11)).isInstanceOf(DeleteSnowflakeCommunitySource.class);
  }

  /**
   * Tests illegal upgrade from OSS to EE
   */
  @Test
  public void testIllegalUpgrade() throws Exception {
    final ByteString prevEdition = ByteString.copyFrom("OSS".getBytes());
    final ConfigurationEntry configurationEntry = new ConfigurationEntry();
    configurationEntry.setValue(prevEdition);
    final LegacyKVStoreProvider kvStoreProvider =
        LegacyKVStoreProviderAdapter.inMemory(CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();
    final ConfigurationStore configurationStore = new ConfigurationStore(kvStoreProvider);
    configurationStore.put(SupportService.DREMIO_EDITION, configurationEntry);
    assertThatThrownBy(() -> new Upgrade(DACConfig.newConfig(), CLASSPATH_SCAN_RESULT, false).validateUpgrade(kvStoreProvider, "EE"))
      .isInstanceOf(Exception.class)
      .hasMessageContaining("Illegal upgrade from OSS to EE");
  }

  /**
   * Test legal upgrade if prior dremio edition is not specified or editions match
   */
  @Test
  public void testLegalUpgrade() throws Exception {
    final ByteString prevEdition = ByteString.copyFrom("OSS".getBytes());
    final ConfigurationEntry configurationEntry = new ConfigurationEntry();
    configurationEntry.setValue(prevEdition);
    final LegacyKVStoreProvider kvStoreProvider =
        LegacyKVStoreProviderAdapter.inMemory(CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();
    final ConfigurationStore configurationStore = new ConfigurationStore(kvStoreProvider);
    new Upgrade(DACConfig.newConfig(), CLASSPATH_SCAN_RESULT, false).validateUpgrade(kvStoreProvider, "OSS");
    configurationStore.put(SupportService.DREMIO_EDITION, configurationEntry);
    new Upgrade(DACConfig.newConfig(), CLASSPATH_SCAN_RESULT, false).validateUpgrade(kvStoreProvider, "OSS");
  }
}
