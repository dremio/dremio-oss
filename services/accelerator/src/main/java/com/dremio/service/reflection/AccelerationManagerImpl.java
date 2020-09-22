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

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.MeasureType;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.NameAndMeasures;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;
import com.dremio.exec.store.sys.accel.LayoutDefinition.Type;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.PartitionDistributionStrategy;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Exposes the acceleration manager interface to the rest of the system (coordinator side)
 */
public class AccelerationManagerImpl implements AccelerationManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationManagerImpl.class);

  private final Provider<NamespaceService> namespaceService;
  private final Provider<ReflectionAdministrationService.Factory> reflectionAdministrationServiceFactory;
  private final Provider<ReflectionService> reflectionService;

  public AccelerationManagerImpl(
    Provider<ReflectionService> reflectionService,
    Provider<ReflectionAdministrationService.Factory> reflectionAdministrationServiceFactory,
    Provider<NamespaceService> namespaceService) {
    super();
    this.reflectionService = reflectionService;
    this.namespaceService = namespaceService;
    this.reflectionAdministrationServiceFactory = reflectionAdministrationServiceFactory;
  }

  @Override
  public ExcludedReflectionsProvider getExcludedReflectionsProvider() {
    return reflectionService.get().getExcludedReflectionsProvider();
  }

  @Override
  public void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound) {
  }

  @Override
  public void addLayout(List<String> path, LayoutDefinition definition, ReflectionContext reflectionContext) {
    final NamespaceKey key = new NamespaceKey(path);
    final DatasetConfig dataset;

    try {
      dataset = namespaceService.get().getDataset(key);
      if(dataset == null) {
        throw UserException.validationError().message("Unable to find requested dataset %s.", key).build(logger);
      }
    } catch(Exception e) {
      throw UserException.validationError(e).message("Unable to find requested dataset %s.", key).build(logger);
    }

    ReflectionGoal goal = new ReflectionGoal();
    ReflectionDetails details = new ReflectionDetails();
    goal.setDetails(details);
    details.setDimensionFieldList(toDimensionFields(definition.getDimension()));
    details.setDisplayFieldList(toDescriptor(definition.getDisplay()));
    details.setDistributionFieldList(toDescriptor(definition.getDistribution()));
    details.setMeasureFieldList(toMeasureFields(definition.getMeasure()));
    details.setPartitionFieldList(toDescriptor(definition.getPartition()));
    details.setSortFieldList(toDescriptor(definition.getSort()));
    details.setPartitionDistributionStrategy(definition.getPartitionDistributionStrategy() ==
        com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy.STRIPED ?
        PartitionDistributionStrategy.STRIPED : PartitionDistributionStrategy.CONSOLIDATED);

    goal.setName(definition.getName());
    goal.setArrowCachingEnabled(definition.getArrowCachingEnabled());
    goal.setState(ReflectionGoalState.ENABLED);
    goal.setType(definition.getType() == Type.AGGREGATE ? ReflectionType.AGGREGATION : ReflectionType.RAW);
    goal.setDatasetId(dataset.getId().getId());

    reflectionAdministrationServiceFactory.get().get(reflectionContext).create(goal);
  }

  private List<ReflectionDimensionField> toDimensionFields(List<SqlCreateReflection.NameAndGranularity> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<SqlCreateReflection.NameAndGranularity, ReflectionDimensionField>() {
          @Nullable
          @Override
          public ReflectionDimensionField apply(@Nullable final SqlCreateReflection.NameAndGranularity input) {
            final DimensionGranularity granularity = input.getGranularity() == SqlCreateReflection.Granularity.BY_DAY ? DimensionGranularity.DATE : DimensionGranularity.NORMAL;
            return new ReflectionDimensionField()
                .setName(input.getName())
                .setGranularity(granularity);
          }
        })
        .toList();
  }

  private List<ReflectionField> toDescriptor(List<String> fields){
    if(fields == null){
      return ImmutableList.of();
    }

    return FluentIterable.from(fields).transform(new Function<String, ReflectionField>(){

      @Override
      public ReflectionField apply(String input) {
        return new ReflectionField(input);
      }}).toList();
  }

  private List<ReflectionMeasureField> toMeasureFields(List<NameAndMeasures> fields){
    if(fields == null){
      return ImmutableList.of();
    }

    return FluentIterable.from(fields).transform(new Function<NameAndMeasures, ReflectionMeasureField>(){

      @Override
      public ReflectionMeasureField apply(NameAndMeasures input) {
        return new ReflectionMeasureField(input.getName())
            .setMeasureTypeList(input.getMeasureTypes().stream()
                .map(AccelerationManagerImpl::toMeasureType)
                .collect(Collectors.toList()));
      }}).toList();
  }

  private static com.dremio.service.reflection.proto.MeasureType toMeasureType(MeasureType t){
    switch(t) {
    case APPROX_COUNT_DISTINCT:
      return com.dremio.service.reflection.proto.MeasureType.APPROX_COUNT_DISTINCT;
    case COUNT:
      return com.dremio.service.reflection.proto.MeasureType.COUNT;
    case MAX:
      return com.dremio.service.reflection.proto.MeasureType.MAX;
    case MIN:
      return com.dremio.service.reflection.proto.MeasureType.MIN;
    case SUM:
      return com.dremio.service.reflection.proto.MeasureType.SUM;
    case UNKNOWN:
    default:
      throw new UnsupportedOperationException(t.name());

    }
  }

  @Override
  public void addExternalReflection(String name, List<String> table, List<String> targetTable, ReflectionContext reflectionContext) {
    reflectionAdministrationServiceFactory.get().get(reflectionContext).createExternalReflection(name, table, targetTable);
  }

  @Override
  public void dropLayout(List<String> path, final String layoutIdOrName, ReflectionContext reflectionContext) {
    NamespaceKey key = new NamespaceKey(path);
    ReflectionAdministrationService administrationReflectionService = reflectionAdministrationServiceFactory.get().get(reflectionContext);
    for (ReflectionGoal rg : administrationReflectionService.getReflectionsByDatasetPath(key)) {
      if (rg.getId().getId().equals(layoutIdOrName) || layoutIdOrName.equals(rg.getName())) {
        administrationReflectionService.remove(rg);
        // only match first and exist.
        return;
      }
    }

    Optional<ExternalReflection> er = Iterables.tryFind(administrationReflectionService.getExternalReflectionByDatasetPath(path), new Predicate<ExternalReflection>() {
      @Override
      public boolean apply(@Nullable ExternalReflection externalReflection) {
        return layoutIdOrName.equalsIgnoreCase(externalReflection.getName()) ||
          layoutIdOrName.equals(externalReflection.getId());
      }
    });

    if (er.isPresent()) {
      administrationReflectionService.dropExternalReflection(er.get().getId());
      return;
    }
    throw UserException.validationError().message("No matching reflection found.").build(logger);
  }

  @Override
  public void toggleAcceleration(List<String> path, Type type, boolean enable, ReflectionContext reflectionContext) {
    Exception ex = null;
    ReflectionAdministrationService administrationReflectionService = reflectionAdministrationServiceFactory.get().get(reflectionContext);
    for(ReflectionGoal g : administrationReflectionService.getReflectionsByDatasetPath(new NamespaceKey(path))) {
      if(
          (type == Type.AGGREGATE && g.getType() != ReflectionType.AGGREGATION) ||
          (type == Type.RAW && g.getType() != ReflectionType.RAW) ||
          (g.getState() == ReflectionGoalState.ENABLED && enable) ||
          (g.getState() == ReflectionGoalState.DISABLED && !enable)
          ) {
        continue;
      }

      try {
        g.setState(enable ? ReflectionGoalState.ENABLED : ReflectionGoalState.DISABLED);
        administrationReflectionService.update(g);
      } catch(Exception e) {
        if(ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }

    if(ex != null) {
      throw UserException.validationError(ex).message("Unable to toggle acceleration, already in requested state.").build(logger);
    }
  }

  @Override
  public void replanlayout(String layoutId) {
  }

  @Override
  public AccelerationDetailsPopulator newPopulator() {
    return new ReflectionDetailsPopulatorImpl(namespaceService.get(), reflectionService.get());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(ReflectionService.class.isAssignableFrom(clazz)) {
      return (T) reflectionService.get();
    }
    return null;
  }
}
