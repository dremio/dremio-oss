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
package com.dremio.dac.support;

import com.dremio.dac.proto.model.source.UpgradeStatus;
import com.dremio.dac.proto.model.source.UpgradeTaskId;
import com.dremio.dac.proto.model.source.UpgradeTaskRun;
import com.dremio.dac.proto.model.source.UpgradeTaskStore;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** KVStore to store Upgrade Tasks */
public class UpgradeStore {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(UpgradeStore.class);

  private static final String TABLE_NAME = "upgrade";

  private LegacyKVStore<UpgradeTaskId, UpgradeTaskStore> store;

  public UpgradeStore(LegacyKVStoreProvider storeProvider) {
    // by this time store should be created
    store = storeProvider.getStore(UpgradeTaskStoreCreator.class);
  }

  public UpgradeTaskStore createUpgradeTaskStoreEntry(
      String upgradeTaskID, String upgradeTaskName, UpgradeTaskRun upgradeTaskRun) {
    UpgradeTaskId upgradeTaskId = new UpgradeTaskId();
    upgradeTaskId.setId(upgradeTaskID);
    UpgradeTaskStore upgradeTaskStore = new UpgradeTaskStore();
    upgradeTaskStore.setId(upgradeTaskId);
    upgradeTaskStore.setName(upgradeTaskName);
    upgradeTaskStore.setRunsList(ImmutableList.of(upgradeTaskRun));
    store.put(upgradeTaskId, upgradeTaskStore);
    return getByTaskId(upgradeTaskID);
  }

  public UpgradeTaskStore getByTaskId(String upgradeTaskID) {
    Preconditions.checkNotNull(upgradeTaskID, "Upgrade Task ID has to be provided");
    UpgradeTaskId upgradeTaskId = new UpgradeTaskId();
    upgradeTaskId.setId(upgradeTaskID);
    return store.get(upgradeTaskId);
  }

  public UpgradeTaskStore addUpgradeRun(
      String upgradeTaskID, String upgradeTaskName, UpgradeTaskRun upgradeTaskRun) {
    Preconditions.checkNotNull(
        upgradeTaskRun,
        "UpgradeTaskRun information should be present for task: " + upgradeTaskName);
    UpgradeTaskId upgradeTaskId = new UpgradeTaskId();
    upgradeTaskId.setId(upgradeTaskID);
    UpgradeTaskStore upgradeTaskStore = store.get(upgradeTaskId);
    if (upgradeTaskStore == null) {
      return createUpgradeTaskStoreEntry(upgradeTaskID, upgradeTaskName, upgradeTaskRun);
    }
    // make sure startTime and endTime is set if upgradeStatus is a terminal state
    if (isTerminalState(upgradeTaskRun.getStatus())) {
      Preconditions.checkState(
          (upgradeTaskRun.getStartTime() != null),
          String.format(
              "Upgrade Run startTime has to be present for task '%s' in state '%s'",
              upgradeTaskName, upgradeTaskRun.getStatus()));
      Preconditions.checkState(
          (upgradeTaskRun.getEndTime() != null),
          String.format(
              "Upgrade Run endTime has to be present for task '%s' in state '%s'",
              upgradeTaskName, upgradeTaskRun.getStatus()));
    }
    List<UpgradeTaskRun> runs = upgradeTaskStore.getRunsList();
    if (runs == null) {
      System.out.println("WARN: No Runs found for task: " + upgradeTaskName + " . Adding one.");
      runs = new ArrayList<>(1);
    }
    runs.add(upgradeTaskRun);
    upgradeTaskStore.setRunsList(runs);
    store.put(upgradeTaskId, upgradeTaskStore);
    return getByTaskId(upgradeTaskID);
  }

  public UpgradeTaskStore updateLastUpgradeRun(
      String upgradeTaskID, String upgradeTaskName, UpgradeTaskRun upgradeTaskRun) {
    UpgradeTaskId upgradeTaskId = new UpgradeTaskId();
    upgradeTaskId.setId(upgradeTaskID);
    UpgradeTaskStore upgradeTaskStore = store.get(upgradeTaskId);
    if (upgradeTaskStore == null) {
      return createUpgradeTaskStoreEntry(upgradeTaskID, upgradeTaskName, upgradeTaskRun);
    }

    List<UpgradeTaskRun> runs = upgradeTaskStore.getRunsList();
    if (runs == null || runs.isEmpty()) {
      // this woudl be really strange
      System.out.println("WARN: No Runs found for task: " + upgradeTaskName + " . Adding one.");
      runs = Collections.singletonList(upgradeTaskRun);
    } else {
      final UpgradeTaskRun lastRun = runs.get(runs.size() - 1);

      // should not upgrade run in terminal state
      Preconditions.checkState(
          !isTerminalState(lastRun.getStatus()),
          String.format(
              "Can not upgrade task: '%s' as it is in state: '%s' which is not terminal state",
              upgradeTaskName, lastRun.getStatus()));

      runs.set(runs.size() - 1, upgradeTaskRun);
    }
    upgradeTaskStore.setRunsList(runs);
    store.put(upgradeTaskId, upgradeTaskStore);
    return getByTaskId(upgradeTaskID);
  }

  public boolean isUpgradeTaskCompleted(String upgradeTaskID) {
    UpgradeTaskStore upgradeTaskStore = getByTaskId(upgradeTaskID);
    // task entry is not found
    if (upgradeTaskStore == null) {
      return false;
    }
    List<UpgradeTaskRun> runs = upgradeTaskStore.getRunsList();
    if (runs == null || runs.isEmpty()) {
      return false;
    }
    UpgradeTaskRun lastRun = runs.get(runs.size() - 1);
    if (UpgradeStatus.COMPLETED.equals(lastRun.getStatus())
        || UpgradeStatus.OUTDATED.equals(lastRun.getStatus())) {
      return true;
    }
    return false;
  }

  /**
   * ONLY for testing - we should not be deleting those entries
   *
   * @param upgradeTaskID
   */
  @VisibleForTesting
  public void deleteUpgradeTaskStoreEntry(String upgradeTaskID) {
    UpgradeTaskId upgradeTaskId = new UpgradeTaskId();
    upgradeTaskId.setId(upgradeTaskID);
    store.delete(upgradeTaskId);
  }

  public List<UpgradeTaskStore> getAllUpgradeTasks() {
    return StreamSupport.stream(store.find().spliterator(), false)
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    List<UpgradeTaskStore> upgradeTaskStores = getAllUpgradeTasks();
    StringBuilder strB = new StringBuilder();
    for (UpgradeTaskStore task : upgradeTaskStores) {
      strB.append(toString(task));
    }
    return strB.toString();
  }

  public String toString(UpgradeTaskStore upgradeTaskStore) {
    StringBuilder strB = new StringBuilder();
    strB.append("\nTask: ID (");
    strB.append(upgradeTaskStore.getId().getId());
    strB.append("), Name (");
    strB.append(upgradeTaskStore.getName());
    strB.append(")\n");
    for (UpgradeTaskRun run : upgradeTaskStore.getRunsList()) {
      strB.append("\t Status: ");
      strB.append(run.getStatus());
      strB.append("\t StartTime : ");
      if (run.getStartTime() != null) {
        strB.append(run.getStartTime());
        strB.append("(");
        strB.append(new Date(run.getStartTime()));
        strB.append(")");
      } else {
        strB.append("not set");
      }
      strB.append("\t EndTime : ");
      if (run.getEndTime() != null) {
        strB.append(run.getEndTime());
        strB.append("(");
        strB.append(new Date(run.getEndTime()));
        strB.append(")");
      } else {
        strB.append("not set");
      }
    }
    return strB.toString();
  }

  public boolean isTerminalState(UpgradeStatus upgradeStatus) {
    switch (upgradeStatus) {
      case RUNNING:
      case UNKNOWN:
        return false;
      case FAILED:
      case COMPLETED:
      case OUTDATED:
        return true;
      default:
        return false;
    }
  }

  /** UpgradeTaskStoreCreator */
  public static class UpgradeTaskStoreCreator
      implements LegacyKVStoreCreationFunction<UpgradeTaskId, UpgradeTaskStore> {

    @Override
    public LegacyKVStore<UpgradeTaskId, UpgradeTaskStore> build(
        LegacyStoreBuildingFactory factory) {
      return factory
          .<UpgradeTaskId, UpgradeTaskStore>newStore()
          .name(TABLE_NAME)
          .keyFormat(Format.ofProtostuff(UpgradeTaskId.class))
          .valueFormat(Format.ofProtostuff(UpgradeTaskStore.class))
          .versionExtractor(UpgradeTaskVersion.class)
          .build();
    }
  }

  /** UpgradeTaskVersion */
  private static final class UpgradeTaskVersion implements VersionExtractor<UpgradeTaskStore> {
    @Override
    public String getTag(UpgradeTaskStore value) {
      return value.getTag();
    }

    @Override
    public void setTag(UpgradeTaskStore value, String tag) {
      value.setTag(tag);
    }
  }
}
