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

import java.util.Set;
import java.util.UUID;

import javax.inject.Provider;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.Strings;

import com.dremio.common.Version;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.service.DirectProvider;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Marks materializations created before 2.0 as deprecated.
 */
public class MarkOldMaterializationsAsDeprecated extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "b2bda6ef-6a7a-4f3e-bf69-7e88ada3fa3c";

  public MarkOldMaterializationsAsDeprecated() {
    super("Mark materializations created before 2.0 as deprecated", ImmutableList.of(SetAccelerationRefreshGrace.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_150;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final Provider<KVStoreProvider> provider = DirectProvider.wrap(context.getKVStoreProvider());
    final com.dremio.service.accelerator.store.MaterializationStore oldStore
        = new com.dremio.service.accelerator.store.MaterializationStore(provider);
    final MaterializationStore newStore = new MaterializationStore(provider);

    oldStore.start();

    int numDeprecated = 0;
    final Set<LayoutId> oldLayoutsToDelete = Sets.newHashSet();

    // lots of paranoid checks below

    for (final MaterializedLayout oldLayout : oldStore.find()) {
      if (oldLayout == null) {
        continue;
      }

      if (oldLayout.getLayoutId() == null || Strings.isNullOrEmpty(oldLayout.getLayoutId().getId())) {
        System.out.println("  Layout id for materialized layout is null/empty, skipping");
        continue;
      }

      if (oldLayout.getMaterializationList() == null || oldLayout.getMaterializationList().isEmpty()) {
        System.out.printf("  Materialization list for materialized layout [%s] is null/empty, skipping\n",
          oldLayout.getLayoutId());
        continue;
      }

      try {
        numDeprecated += deprecateMaterializedLayout(newStore, oldLayout);
      } catch (Exception e) {
        System.out.printf("  Failed to handle materialized layout [%s], skipping%n    %s%n",
          oldLayout.getLayoutId().getId(), e.getMessage());
      }

      oldLayoutsToDelete.add(oldLayout.getLayoutId());
    }

    System.out.printf("  Deprecated %d materializations. These will be deleted after configured grace period\n",
      numDeprecated);

    // delete all old layouts
    for (LayoutId layoutId : oldLayoutsToDelete) {
      oldStore.remove(layoutId);
    }

    System.out.printf("  Removed %d from the old materialization store\n", oldLayoutsToDelete.size());
  }

  private int deprecateMaterializedLayout(MaterializationStore newStore, MaterializedLayout oldLayout) {
    long seriesId = 0; // since all of these belong to the same reflection/layout
    int numDeprecated = 0;
    for (final com.dremio.service.accelerator.proto.Materialization oldMaterialization :
      oldLayout.getMaterializationList()) {

      if (oldMaterialization == null ||
          oldMaterialization.getId() == null ||
          Strings.isNullOrEmpty(oldMaterialization.getId().getId())) {
        System.out.printf("  Materialization id is null/empty (list size [%d]), layout [%s], skipping\n",
          oldLayout.getMaterializationList().size(), oldLayout.getLayoutId());
        continue;
      }
      final MaterializationId materializationId = new MaterializationId(oldMaterialization.getId().getId());

      if (oldMaterialization.getLayoutId() == null || Strings.isNullOrEmpty(oldMaterialization.getLayoutId().getId())) {
        // although we know the layout id from outer loop, skip to avoid trouble
        System.out.printf("  Layout id is null/empty, materialization [%s], layout [%s] (size [%d]), skipping\n",
          oldMaterialization.getId(), oldLayout.getLayoutId(), oldLayout.getMaterializationList().size());
        continue;
      }
      // "layout" is now "reflection"
      final ReflectionId reflectionId = new ReflectionId(oldMaterialization.getLayoutId().getId());

      final Materialization newMaterialization = new Materialization()
        .setId(materializationId)
        .setReflectionId(reflectionId)
        .setState(MaterializationState.DEPRECATED)
        .setSeriesId(seriesId)
        .setLegacyReflectionGoalVersion(0L)
        .setSeriesOrdinal(0);

      final RefreshId refreshId = new RefreshId(UUID.randomUUID().toString());
      final String path = StringUtils.join(Iterables.skip(
        ReflectionUtils.getMaterializationPath(newMaterialization), 1), "/");

      final Refresh newRefresh = new Refresh()
        .setId(refreshId)
        .setReflectionId(reflectionId)
        .setSeriesId(seriesId)
        .setSeriesOrdinal(0)
        .setPath(path);

      System.out.printf("  Marking materialization with id [%s] (for reflection [%s], series [%d]) as deprecated\n",
        materializationId, reflectionId, seriesId);
      newStore.save(newMaterialization);
      newStore.save(newRefresh);

      seriesId++;
      numDeprecated++;
    }

    return numDeprecated;
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
