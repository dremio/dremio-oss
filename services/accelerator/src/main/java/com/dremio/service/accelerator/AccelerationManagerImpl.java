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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.parser.SqlAddLayout;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;
import com.dremio.exec.store.sys.accel.LayoutDefinition.Type;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMetrics;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationType;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.PartitionDistributionStrategy;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Exposes the acceleration manager interface to the rest of the system (coordinator side)
 */
public class AccelerationManagerImpl implements AccelerationManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationManagerImpl.class);

  private final NamespaceService namespace;
  private final AccelerationService accelService;

  public AccelerationManagerImpl(AccelerationService accelService, NamespaceService namespace) {
    super();
    this.accelService = accelService;
    this.namespace = namespace;
  }

  @Override
  public void addAcceleration(List<String> path) {
    final NamespaceKey namespaceKey = new NamespaceKey(path);
    if(accelService.getAccelerationEntryByDataset(namespaceKey).isPresent()){
      throw UserException.validationError().message("Acceleration already created.").build(logger);
    }

    try{
      final DatasetConfig config = namespace.getDataset(namespaceKey);

      final Acceleration acceleration = new Acceleration()
          .setId(new AccelerationId(config.getId().getId()))
          .setType(AccelerationType.DATASET)
          .setState(AccelerationState.NEW_ADMIN)
          .setContext(new AccelerationContext()
              .setDatasetSchema(new RowType())
              .setLogicalAggregation(new LogicalAggregation())
              .setDataset(config)
              )
          .setAggregationLayouts(new LayoutContainer().setType(LayoutType.AGGREGATION))
          .setRawLayouts(new LayoutContainer().setType(LayoutType.RAW))
          .setMetrics(new AccelerationMetrics())
          .setPipeline(new AccelerationPipeline());

      accelService.create(acceleration);
    } catch(NamespaceException ex){
      throw UserException.dataReadError(ex).message("Failed to add acceleration to provided path.").build(logger);
    }
  }

  @Override
  public void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound) {
    AccelerationEntry entry = get(path, raiseErrorIfNotFound);
    if (entry == null) {
      return;
    }
    accelService.remove(entry.getDescriptor().getId());
  }

  @Override
  public void addLayout(List<String> path, LayoutDefinition definition) {
    AccelerationEntry entry = get(path);

    LayoutDetailsDescriptor details = new LayoutDetailsDescriptor();
    details.setDimensionFieldList(toDimensionFields(definition.getDimension()));
    details.setDisplayFieldList(toDescriptor(definition.getDisplay()));
    details.setDistributionFieldList(toDescriptor(definition.getDistribution()));
    details.setMeasureFieldList(toDescriptor(definition.getMeasure()));
    details.setPartitionFieldList(toDescriptor(definition.getPartition()));
    details.setSortFieldList(toDescriptor(definition.getSort()));
    details.setPartitionDistributionStrategy(definition.getPartitionDistributionStrategy() ==
        com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy.STRIPED ?
        PartitionDistributionStrategy.STRIPED : PartitionDistributionStrategy.CONSOLIDATED);
    LayoutDescriptor descriptor = new LayoutDescriptor(details)
        .setName(definition.getName());
    entry.getDescriptor().setMode(AccelerationMode.MANUAL);
    if(definition.getType() == Type.AGGREGATE){
      List<LayoutDescriptor> descriptors = new ArrayList<>();
      descriptors.addAll(entry.getDescriptor().getAggregationLayouts().getLayoutList());
      descriptors.add(descriptor);
      entry.getDescriptor().getAggregationLayouts().setLayoutList(descriptors);
    } else {
      List<LayoutDescriptor> descriptors = new ArrayList<>();
      descriptors.addAll(entry.getDescriptor().getRawLayouts().getLayoutList());
      descriptors.add(descriptor);
      entry.getDescriptor().getRawLayouts().setLayoutList(descriptors);
    }

    accelService.update(entry);
  }

  private List<LayoutDimensionFieldDescriptor> toDimensionFields(List<SqlAddLayout.NameAndGranularity> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<SqlAddLayout.NameAndGranularity, LayoutDimensionFieldDescriptor>() {
          @Nullable
          @Override
          public LayoutDimensionFieldDescriptor apply(@Nullable final SqlAddLayout.NameAndGranularity input) {
            final DimensionGranularity granularity = input.getGranularity() == SqlAddLayout.Granularity.BY_DAY ? DimensionGranularity.DATE : DimensionGranularity.NORMAL;
            return new LayoutDimensionFieldDescriptor()
                .setName(input.getName())
                .setGranularity(granularity);
          }
        })
        .toList();
  }

  private List<LayoutFieldDescriptor> toDescriptor(List<String> fields){
    if(fields == null){
      return ImmutableList.of();
    }

    return FluentIterable.from(fields).transform(new Function<String, LayoutFieldDescriptor>(){

      @Override
      public LayoutFieldDescriptor apply(String input) {
        return new LayoutFieldDescriptor(input);
      }}).toList();
  }

  @Override
  public void dropLayout(List<String> path, String layoutId) {
    AccelerationEntry entry = get(path);
    throw UserException.unsupportedError().message("Dropping layouts not yet supported.").build(logger);
  }

  @Override
  public void toggleAcceleration(List<String> path, Type type, boolean enable) {
    AccelerationEntry entry = get(path);
    LayoutContainerDescriptor descriptor = type == Type.RAW ? entry.getDescriptor().getRawLayouts() : entry.getDescriptor().getAggregationLayouts();
    if(descriptor.getEnabled() == enable){
      throw UserException.validationError().message("Unable to toggle acceleration, already in requested state.").build(logger);
    }
    descriptor.setEnabled(enable);
    accelService.update(entry);
  }

  private AccelerationEntry get(List<String> path) {
    return get(path, true);
  }

  private AccelerationEntry get(List<String> path, boolean raiseErrorIfNotFound) {
    Optional<AccelerationEntry> entry = accelService.getAccelerationEntryByDataset(new NamespaceKey(path));
    if(!entry.isPresent() && raiseErrorIfNotFound){
      throw UserException.validationError().message("Table is not currently accelerated.").build(logger);
    }

    return entry.orNull();
  }

  @Override
  public void replanlayout(String layoutId) {
    accelService.replan(new LayoutId(layoutId));
  }

  @Override
  public AccelerationDetailsPopulator newPopulator() {
    return new AccelerationDetailsPopulatorImpl(accelService);
  }
}
