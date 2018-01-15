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

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.DatasetDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * populates a {@link AccelerationDetails}
 */
public class AccelerationDetailsPopulatorImpl implements AccelerationDetailsPopulator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationDetailsPopulatorImpl.class);

  /**
   * Internal class used to track reflections used during planning
   */
  private class ReflectionState {
    private final String materializationId;
    private final String layoutId;
    private final boolean matched;
    private boolean chosen;

    ReflectionState(String materializationId, String layoutId, boolean matched) {
      this.materializationId = Preconditions.checkNotNull(materializationId, "materializationId cannot be null");
      this.layoutId = Preconditions.checkNotNull(layoutId, "layoutId cannot be null");
      this.matched = matched;
    }

    SubstitutionState getSubstitutionState() {
      if (chosen) {
        return SubstitutionState.CHOSEN;
      } else if (matched) {
        return SubstitutionState.MATCHED;
      }
      return SubstitutionState.CONSIDERED;
    }
  }

  private final AccelerationService accelerationService;
  private final AccelerationDetails details = new AccelerationDetails();
  private final Map<String, ReflectionState> consideredReflections = Maps.newHashMap();

  AccelerationDetailsPopulatorImpl(AccelerationService accelerationService) {
    this.accelerationService = accelerationService;
  }

  @Override
  public void planSubstituted(DremioRelOptMaterialization materialization, List<RelNode> substitutions, RelNode target, long millisTaken) {
    try {
      // reflection was considered and matched
      if (!consideredReflections.containsKey(materialization.getLayoutId())) {
        final ReflectionState state = new ReflectionState(
          materialization.getMaterializationId(),
          materialization.getLayoutId(),
          !substitutions.isEmpty() // non empty substitutions means that the reflected was matched at least once
        );
        consideredReflections.put(materialization.getLayoutId(), state);
      }
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to handle planSubstituted()", e);
    }
  }

  @Override
  public void planAccelerated(SubstitutionInfo info) {
    try {
      for (SubstitutionInfo.Substitution sub : info.getSubstitutions()) {
        final String layoutId = sub.getMaterialization().getLayoutId();
        if (consideredReflections.containsKey(layoutId)) {
          consideredReflections.get(sub.getMaterialization().getLayoutId()).chosen = true;
        }
      }
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to handle planAccelerated()", e);
    }
  }

  @Override
  public byte[] computeAcceleration() {
    try {
      if (!consideredReflections.isEmpty()) {
        List<ReflectionRelationship> relationships = Lists.newArrayList();
        for (final ReflectionState reflectionState : consideredReflections.values()) {
          final Optional<Layout> layout = accelerationService.getLayout(new LayoutId(reflectionState.layoutId));
          if (!layout.isPresent()) {
            continue; // layout no longer present, ignore
          }

          final Materialization materialization = Iterables.find(accelerationService.getMaterializations(layout.get().getId()),
            new Predicate<Materialization>() {
              @Override
              public boolean apply(Materialization input) {
                return input.getId().getId().equals(reflectionState.materializationId);
              }
            });
          final String materializationId = materialization == null ? null : materialization.getId().getId();
          // for materializations done prior to 1.3.0 the refreshChainStartTime field will be null
          long refreshChainStartTime = 0L;
          if (materialization != null && materialization.getRefreshChainStartTime() != null) {
            refreshChainStartTime = materialization.getRefreshChainStartTime();
          }

          final Optional<com.dremio.service.accelerator.proto.Acceleration> acceleration =
            accelerationService.getAccelerationByLayoutId(layout.get().getId());
          if (!acceleration.isPresent()) {
            continue; // acceleration no longer present, ignore
          }

          DatasetConfig datasetConfig = acceleration.get().getContext().getDataset();

          final LayoutDescriptor layoutDescriptor = AccelerationMapper.toLayoutDescriptor(layout.get());

          // not all datasets have acceleration settings
          final PhysicalDataset physicalDataset = datasetConfig.getPhysicalDataset();
          final AccelerationSettings accelerationSettings = physicalDataset != null ? physicalDataset.getAccelerationSettings() : null;

          relationships.add(new ReflectionRelationship()
            .setState(reflectionState.getSubstitutionState())
            .setMaterialization(new MaterializationDetails()
              .setId(materializationId)
              .setRefreshChainStartTime(refreshChainStartTime))
            .setDataset(new DatasetDetails()
              .setId(datasetConfig.getId().getId())
              .setPathList(datasetConfig.getFullPathList())
              .setType(datasetConfig.getType()))
            .setAccelerationSettings(accelerationSettings)
            .setReflectionType(layout.get().getLayoutType())
            .setReflection(layoutDescriptor)
          );
        }

        details.setReflectionRelationshipsList(relationships);
      }
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to compute the acceleration", e);
    }

    return AccelerationDetailsUtils.serialize(details);
  }

}
