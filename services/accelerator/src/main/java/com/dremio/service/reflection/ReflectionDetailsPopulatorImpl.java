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
package com.dremio.service.reflection;

import static com.dremio.service.reflection.ReflectionUtils.isPhysicalDataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.sabot.kernel.proto.ReflectionExplanation;
import com.dremio.service.accelerator.AccelerationDetailsUtils;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.DatasetDetails;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutMeasureFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.MaterializationDetails;
import com.dremio.service.accelerator.proto.MeasureType;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * populates a {@link AccelerationDetails}
 */
class ReflectionDetailsPopulatorImpl implements AccelerationDetailsPopulator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionDetailsPopulatorImpl.class);

  private final NamespaceService namespace;
  private final ReflectionService reflections;
  private final AccelerationDetails details = new AccelerationDetails();
  private final Map<String, ReflectionState> consideredReflections = new HashMap<>();
  private List<String> substitutionErrors = Collections.emptyList();
  private final List<String> consideredReflectionIds = new ArrayList<>();
  private final List<String> matchedReflectionIds = new ArrayList<>();
  private final List<String> chosenReflectionIds = new ArrayList<>();

  ReflectionDetailsPopulatorImpl(NamespaceService namespace, ReflectionService reflections) {
    this.reflections = reflections;
    this.namespace = namespace;
  }

  @Override
  public void planSubstituted(DremioMaterialization materialization, List<RelNode> substitutions, RelNode target, long millisTaken, boolean defaultReflection) {
    try {
      // reflection was considered and matched
      final ReflectionState state = new ReflectionState(
        materialization.getMaterializationId(),
        materialization.getReflectionId(),
        !substitutions.isEmpty(), // non empty substitutions means that the reflected was matched at least once
        materialization.isSnowflake()
      );
      consideredReflections.put(materialization.getReflectionId(), state);
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to handle planSubstituted()", e);
    }
  }

  @Override
  public void addReflectionHints(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    try {
      // reflection was considered and matched
      if (consideredReflections.containsKey(reflectionExplanationsAndQueryDistance.getReflectionId())
        && consideredReflections.get(reflectionExplanationsAndQueryDistance.getReflectionId()).getSubstitutionState() == SubstitutionState.CONSIDERED) {
        ReflectionState cr = consideredReflections.get(reflectionExplanationsAndQueryDistance.getReflectionId());
        cr.queryDistance = reflectionExplanationsAndQueryDistance.getQueryDistance();
        cr.explanations = reflectionExplanationsAndQueryDistance.getDisplayHintMessageList();
        cr.hideHint = reflectionExplanationsAndQueryDistance.isHintHidden();
      }
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to handle planSubstituted()", e);
    }
  }

  @Override
  public void attemptCompleted(QueryProfile profile) {
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    if (errors != null) {
      substitutionErrors = Lists.newArrayList(errors);
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

  private AccelerationSettings getAccelerationSettings(DatasetConfig config) {
    final ReflectionSettings reflectionSettings = reflections.getReflectionSettings();
    final NamespaceKey datasetKey = new NamespaceKey(config.getFullPathList());
    // not all datasets have acceleration settings
    return isPhysicalDataset(config.getType()) ? reflectionSettings.getReflectionSettings(datasetKey) : null;
  }

  @Override
  public byte[] computeAcceleration() {
    try {
      if (!consideredReflections.isEmpty()) {
        List<ReflectionRelationship> relationships = Lists.newArrayList();
        for (final ReflectionState reflectionState : consideredReflections.values()) {
          final Optional<ReflectionGoal> reflectionOptional = reflections.getGoal(new ReflectionId(reflectionState.reflectionId));
          if (reflectionOptional.isPresent()) {
            final ReflectionGoal reflection = reflectionOptional.get();
            final Optional<Materialization> materialization = reflections.getMaterialization(new MaterializationId(reflectionState.materializationId));
            final String materializationId;
            final long refreshChainStartTime;
            if(materialization.isPresent()) {
              materializationId = materialization.get().getId().getId();
              if(materialization.get().getLastRefreshFromPds() != null) {
                refreshChainStartTime = materialization.get().getLastRefreshFromPds();
              } else {
                refreshChainStartTime = 0;
              }
            } else {
              materializationId = null;
              refreshChainStartTime = 0;
            }

            DatasetConfig datasetConfig = namespace.findDatasetByUUID(reflection.getDatasetId());
            if(datasetConfig == null) {
              continue;
            }

            final LayoutDescriptor layoutDescriptor = toLayoutDescriptor(reflection);

            final AccelerationSettings settings = getAccelerationSettings(datasetConfig);

            relationships.add(new ReflectionRelationship()
              .setState(reflectionState.getSubstitutionState())
              .setMaterialization(new MaterializationDetails()
                .setId(materializationId)
                .setRefreshChainStartTime(refreshChainStartTime))
              .setDataset(new DatasetDetails()
                .setId(datasetConfig.getId().getId())
                .setPathList(datasetConfig.getFullPathList())
                .setType(datasetConfig.getType()))
              .setAccelerationSettings(settings)
              .setReflectionType(reflection.getType() == ReflectionType.RAW ? com.dremio.service.accelerator.proto.LayoutType.RAW : com.dremio.service.accelerator.proto.LayoutType.AGGREGATION)
              .setReflection(layoutDescriptor)
              .setSnowflake(reflectionState.snowflake)
              .setQueryDistance(reflectionState.queryDistance)
              .setReflectionExplanationList(reflectionState.explanations)
              .setHideHint(reflectionState.hideHint)
            );
          } else {
            // maybe its a external reflections?
            Optional<ExternalReflection> externalReflectionOptional = reflections.getExternalReflectionById(reflectionState.reflectionId);
            if (!externalReflectionOptional.isPresent()) {
              continue; // layout no longer present, ignore
            }

            final ExternalReflection externalReflection = externalReflectionOptional.get();

            DatasetConfig datasetConfig = namespace.findDatasetByUUID(externalReflection.getQueryDatasetId());
            if(datasetConfig == null) {
              continue;
            }

            LayoutDescriptor layoutDescriptor = new LayoutDescriptor()
              .setId(new LayoutId(externalReflection.getId()))
              .setName(externalReflection.getName())
              .setDetails(new LayoutDetailsDescriptor());

            relationships.add(new ReflectionRelationship()
              .setState(reflectionState.getSubstitutionState())
              .setMaterialization(new MaterializationDetails())
              .setDataset(new DatasetDetails()
                .setId(datasetConfig.getId().getId())
                .setPathList(datasetConfig.getFullPathList())
                .setType(datasetConfig.getType()))
              .setAccelerationSettings(null)
              .setReflectionType(LayoutType.EXTERNAL)
              .setReflection(layoutDescriptor)
              .setQueryDistance(reflectionState.queryDistance)
              .setReflectionExplanationList(reflectionState.explanations)
              .setHideHint(reflectionState.hideHint)
            );
          }
          if (reflectionState.getSubstitutionState() == SubstitutionState.CONSIDERED) {
            consideredReflectionIds.add(reflectionState.reflectionId);
          } else if (reflectionState.getSubstitutionState() == SubstitutionState.MATCHED) {
            consideredReflectionIds.add(reflectionState.reflectionId);
            matchedReflectionIds.add(reflectionState.reflectionId);
          } else if (reflectionState.getSubstitutionState() == SubstitutionState.CHOSEN) {
            consideredReflectionIds.add(reflectionState.reflectionId);
            matchedReflectionIds.add(reflectionState.reflectionId);
            chosenReflectionIds.add(reflectionState.reflectionId);
          }
        }

        details.setReflectionRelationshipsList(relationships);
      }
    } catch (Exception e) {
      logger.error("AccelerationDetails populator failed to compute the acceleration", e);
    } finally {
      details.setErrorList(substitutionErrors);
    }

    return AccelerationDetailsUtils.serialize(details);
  }

  @Override
  public List<String> getConsideredReflectionIds() {
    return consideredReflectionIds;
  }

  @Override
  public List<String> getMatchedReflectionIds() {
    return matchedReflectionIds;
  }

  @Override
  public List<String> getChosenReflectionIds() {
    return chosenReflectionIds;
  }

  private static LayoutDescriptor toLayoutDescriptor(final ReflectionGoal layout) {
    final ReflectionDetails details = Preconditions.checkNotNull(layout.getDetails(), "layout details is required");

    return new LayoutDescriptor()
        .setId(new LayoutId(layout.getId().getId()))
        .setName(layout.getName())
        .setDetails(
            new LayoutDetailsDescriptor()
                .setPartitionFieldList(toLayoutFieldDescriptors(details.getPartitionFieldList()))
                .setDimensionFieldList(toLayoutDimensionFieldDescriptors(details.getDimensionFieldList()))
                .setMeasureFieldList(toLayoutMeasureFieldDescriptors(details.getMeasureFieldList()))
                .setSortFieldList(toLayoutFieldDescriptors(details.getSortFieldList()))
                .setDisplayFieldList(toLayoutFieldDescriptors(details.getDisplayFieldList()))
                .setDistributionFieldList(toLayoutFieldDescriptors(details.getDistributionFieldList()))
                .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy() == PartitionDistributionStrategy.CONSOLIDATED ? com.dremio.service.accelerator.proto.PartitionDistributionStrategy.CONSOLIDATED : com.dremio.service.accelerator.proto.PartitionDistributionStrategy.STRIPED)
        );

  }

  private static List<LayoutFieldDescriptor> toLayoutFieldDescriptors(final List<ReflectionField> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<ReflectionField, LayoutFieldDescriptor>() {
          @Override
          public LayoutFieldDescriptor apply(final ReflectionField field) {
            return toLayoutFieldDescriptor(field);
          }
        })
        .toList();
  }


  private static List<LayoutMeasureFieldDescriptor> toLayoutMeasureFieldDescriptors(final List<ReflectionMeasureField> fields) {
    return AccelerationUtils.selfOrEmpty(fields).stream()
        .map(ReflectionDetailsPopulatorImpl::toLayoutMeasureFieldDescriptor)
        .collect(Collectors.toList());
  }


  private static MeasureType toMeasureType(com.dremio.service.reflection.proto.MeasureType t){
    switch(t) {
    case APPROX_COUNT_DISTINCT:
      return MeasureType.APPROX_COUNT_DISTINCT;
    case COUNT:
      return MeasureType.COUNT;
    case MAX:
      return MeasureType.MAX;
    case MIN:
      return MeasureType.MIN;
    case SUM:
      return MeasureType.SUM;
    case UNKNOWN:
    default:
      throw new UnsupportedOperationException(t.name());

    }
  }

  private static LayoutMeasureFieldDescriptor toLayoutMeasureFieldDescriptor(ReflectionMeasureField measureField) {
    return new LayoutMeasureFieldDescriptor(measureField.getName())
        .setMeasureTypeList(AccelerationUtils.selfOrEmpty(measureField.getMeasureTypeList()).stream()
            .map(ReflectionDetailsPopulatorImpl::toMeasureType)
            .collect(Collectors.toList()));
  }

  private static List<LayoutDimensionFieldDescriptor> toLayoutDimensionFieldDescriptors(final List<ReflectionDimensionField> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<ReflectionDimensionField, LayoutDimensionFieldDescriptor>() {
          @Override
          public LayoutDimensionFieldDescriptor apply(final ReflectionDimensionField input) {
            return new LayoutDimensionFieldDescriptor()
                .setName(input.getName())
                .setGranularity(input.getGranularity()== DimensionGranularity.DATE ?
                    com.dremio.service.accelerator.proto.DimensionGranularity.DATE
                    : com.dremio.service.accelerator.proto.DimensionGranularity.NORMAL);
          }
        })
        .toList();
  }

  private static LayoutFieldDescriptor toLayoutFieldDescriptor(final ReflectionField field) {
    return new LayoutFieldDescriptor().setName(field.getName());
  }


  /**
   * Internal class used to track reflections used during planning
   */
  private class ReflectionState {
    private final String materializationId;
    private final String reflectionId;
    private final boolean matched;
    private boolean chosen;
    private boolean hideHint = false;
    private boolean snowflake;
    private double queryDistance = Double.NaN;
    private List<ReflectionExplanation> explanations = ImmutableList.of();

    ReflectionState(String materializationId, String reflectionId, boolean matched, boolean snowflake) {
      this.materializationId = Preconditions.checkNotNull(materializationId, "materializationId cannot be null");
      this.reflectionId = Preconditions.checkNotNull(reflectionId, "layoutId cannot be null");
      this.matched = matched;
      this.snowflake = snowflake;
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


}
