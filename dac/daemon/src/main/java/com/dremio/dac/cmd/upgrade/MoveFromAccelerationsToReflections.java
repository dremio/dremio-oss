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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.store.AccelerationEntryStore;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.google.common.collect.Sets;

/**
 * Upgrades from the old acceleration service to the new reflection service
 */
public class MoveFromAccelerationsToReflections extends UpgradeTask {
  public MoveFromAccelerationsToReflections() {
    super("Migrate from accelerations to reflections", VERSION_106, VERSION_150);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final AccelerationStore accelerationStore = new AccelerationStore(context.getKVStoreProvider());
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    final AccelerationEntryStore entryStore = new AccelerationEntryStore(context.getKVStoreProvider());
    final ReflectionGoalsStore reflectionGoalsStore = new ReflectionGoalsStore(context.getKVStoreProvider());

    accelerationStore.start();
    entryStore.start();

    final Set<AccelerationId> oldAccelerationsToDelete = Sets.newHashSet();

    Iterable<Acceleration> accelerations = accelerationStore.find();
    for (Acceleration acceleration : accelerations) {
      final AccelerationId id = acceleration.getId();
      oldAccelerationsToDelete.add(id);

      final DatasetConfig accelerationDataset = acceleration.getContext().getDataset();

      DatasetConfig datasetConfig = namespaceService.findDatasetByUUID(accelerationDataset.getId().getId());
      // skip accelerations whose datasets no longer exist
      if (datasetConfig == null) {
        System.out.printf("  Could not find dataset [%s] for acceleration [%s], skipping\n", accelerationDataset.getFullPathList(), id.getId());
        continue;
      }

      System.out.printf("  Migrating dataset [%s] with acceleration [%s]\n", accelerationDataset.getFullPathList(), id.getId());

      migrateLayoutContainer(acceleration.getRawLayouts(), reflectionGoalsStore, datasetConfig.getId().getId());
      migrateLayoutContainer(acceleration.getAggregationLayouts(), reflectionGoalsStore, datasetConfig.getId().getId());
    }

    // now clear the old stores
    System.out.println("  Clearing out old stores...");
    for (AccelerationId accelerationId : oldAccelerationsToDelete) {
      accelerationStore.remove(accelerationId);
      entryStore.remove(accelerationId);
    }

    System.out.println("  Clearing out old stores complete");
  }

  private void migrateLayoutContainer(LayoutContainer container, final ReflectionGoalsStore reflectionGoalsStore, String datasetId) {
    if (container == null || container.getLayoutList() == null) {
      return;
    }

    int counter = 0;

    for (Layout layout: container.getLayoutList()) {
      try {
        ReflectionGoal goal = getGoalFromLayout(layout, container.getEnabled(), datasetId, ++counter);
        final ReflectionId reflectionId = new ReflectionId(UUID.randomUUID().toString());
        goal.setId(reflectionId);

        // only migrate if the ReflectionGoal passes validation
        ReflectionUtils.validateReflectionGoalWithoutSchema(goal);

        reflectionGoalsStore.save(goal);
      } catch(Exception e) {
        System.out.printf("    - failed to migrate layout [%s] because of [%s]\n", layout, e);
      }
    }
  }

  private ReflectionGoal getGoalFromLayout(Layout layout, Boolean enabled, String datasetId, int counter) {
    LayoutDetails details = layout.getDetails();

    ReflectionGoal goal = new ReflectionGoal();
    goal.setType(layout.getLayoutType() == LayoutType.AGGREGATION ? ReflectionType.AGGREGATION : ReflectionType.RAW);

    String type = layout.getLayoutType() == LayoutType.AGGREGATION ? "Aggregate" : "Raw";

    if (layout.getName() == null || layout.getName().trim().isEmpty()) {
      goal.setName(String.format("Unnamed %s Reflection %s", type, counter));
    } else {
      goal.setName(layout.getName());
    }

    goal.setDatasetId(datasetId);
    goal.setState(enabled ? ReflectionGoalState.ENABLED : ReflectionGoalState.DISABLED);

    ReflectionDetails reflectionDetails = new ReflectionDetails();

    List<ReflectionDimensionField> dimensionFields = new ArrayList<>();
    if (details.getDimensionFieldList() != null) {
      for (LayoutDimensionField field : details.getDimensionFieldList()) {
        ReflectionDimensionField dimensionField = new ReflectionDimensionField();
        dimensionField.setName(field.getName());
        DimensionGranularity granularity = DimensionGranularity.valueOf(field.getGranularity().getNumber());
        dimensionField.setGranularity(granularity);
        dimensionFields.add(dimensionField);
      }
    }

    reflectionDetails.setDimensionFieldList(dimensionFields);
    reflectionDetails.setPartitionFieldList(getFields(details.getPartitionFieldList()));
    reflectionDetails.setSortFieldList(getFields(details.getSortFieldList()));
    reflectionDetails.setDistributionFieldList(getFields(details.getDistributionFieldList()));
    reflectionDetails.setDisplayFieldList(getFields(details.getDisplayFieldList()));
    reflectionDetails.setMeasureFieldList(getFields(details.getMeasureFieldList()));

    if (details.getPartitionDistributionStrategy() == com.dremio.service.accelerator.proto.PartitionDistributionStrategy.CONSOLIDATED) {
      reflectionDetails.setPartitionDistributionStrategy(PartitionDistributionStrategy.CONSOLIDATED);
    } else {
      reflectionDetails.setPartitionDistributionStrategy(PartitionDistributionStrategy.STRIPED);
    }

    goal.setDetails(reflectionDetails);
    return goal;
  }

  private List<ReflectionField> getFields(List<LayoutField> fields) {
    List<ReflectionField> newFields = new ArrayList<>();
    if (fields != null) {
      for (LayoutField field : fields) {
        newFields.add(new ReflectionField(field.getName()));
      }
    }
    return newFields;
  }
}
