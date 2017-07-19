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
package com.dremio.dac.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.ApiErrorCode;
import com.dremio.dac.proto.model.acceleration.ApiErrorDetails;
import com.dremio.dac.proto.model.acceleration.LayoutApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutContainerApiDescriptor;
import com.dremio.dac.proto.model.acceleration.MaterializationFailureDetails;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.AccelerationServiceImpl;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.JobDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializatonFailure;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Base class for acceleration resource
 */
public class BaseAccelerationResource {
  private static final LayoutContainerApiDescriptor EMPTY_CONTAINER = new LayoutContainerApiDescriptor();

  private static final Logger logger = LoggerFactory.getLogger(AccelerationResource.class);

  private final AccelerationService accelerationService;
  private final NamespaceService namespaceService;

  protected BaseAccelerationResource(AccelerationService accelerationService, NamespaceService namespaceService) {
    this.accelerationService = accelerationService;
    this.namespaceService = namespaceService;
  }

  /**
   * Augments and returns acceleration descriptor with number of requests, error details...
   */
  protected AccelerationApiDescriptor augment(final AccelerationApiDescriptor descriptor) {
    final String messageNotFound = String.format("unable to find acceleration [id: %s]", descriptor.getId().getId());
    final AccelerationStateApiDescriptor state = descriptor.getState();
    final Acceleration acceleration = AccelerationUtils.getOrFailUnchecked(
        accelerationService.getAccelerationById(descriptor.getId()), messageNotFound);

    Map<LayoutId, Layout> layoutMap = getLayoutMap(acceleration);

    calculateAndSetSummaryInfo(acceleration, descriptor, layoutMap);

    if (state == AccelerationStateApiDescriptor.ERROR) {
      // write pipeline failures
      final AccelerationPipeline pipeline = Optional.fromNullable(acceleration.getPipeline())
          .or(new AccelerationPipeline().setFailureDetails(new AccelerationPipeline.FailureDetails()));

      final AccelerationPipeline.FailureDetails details = pipeline.getFailureDetails();
      descriptor
      .setErrorList(ImmutableList.of(new ApiErrorDetails()
          .setCode(ApiErrorCode.PIPELINE_FAILURE)
          .setMessage(details.getMessage())
          .setStackTrace(details.getStackTrace())
          ));
    } else {
      // this is in else block as we do not want to override pipeline failures
      // write materialization failures.
      writeMaterializationFailuresIfAny(descriptor);
    }

    // write number of requests
    final AccelerationEntry entry = AccelerationUtils.getOrFailUnchecked(accelerationService.getAccelerationEntryById(descriptor.getId()), messageNotFound);
    descriptor
    .setTotalRequests(AccelerationUtils.selfOrEmpty(entry.getRequestList()).size())
    .setVersion(entry.getDescriptor().getVersion());

    return descriptor;
  }

  private Map<LayoutId, Layout> getLayoutMap(Acceleration acceleration) {
    Map<LayoutId, Layout> map = new HashMap<>();
    for (Layout layout : AccelerationUtils.getAllLayouts(acceleration)) {
      map.put(layout.getId(), layout);
    }
    return map;
  }

  private void calculateAndSetSummaryInfo(Acceleration acceleration, AccelerationApiDescriptor descriptor, Map<LayoutId, Layout> layoutMap) {
    for (LayoutApiDescriptor layout : getAllLayouts(descriptor)) {
      LayoutId layoutId = layout.getId();
      Optional<Materialization> latestValidMaterialization = getLatestValidMaterialization(layout, layoutMap.get(layoutId));
      if (latestValidMaterialization.isPresent()) {
        layout.setHasValidMaterialization(true);
        layout.setCurrentByteSize(latestValidMaterialization.get().getMetrics().getFootprint());
      }
      Optional<Materialization> lastestMaterialization = getLatestMaterialization(layoutId);
      if (lastestMaterialization.isPresent()) {
        layout.setLatestMaterializationState(lastestMaterialization.get().getState());
      }
      layout.setTotalByteSize(getTotalSize(layoutId));
    }
  }

  private long getTotalSize(LayoutId id) {
    Iterable<Materialization> materializations = accelerationService.getMaterializations(id);
    long totalSize = 0;
    for (Materialization materialization : materializations) {
      if (materialization.getState().equals(MaterializationState.DONE)) {
        totalSize += materialization.getMetrics().getFootprint();
      }
    }
    return totalSize;
  }

  private Optional<Materialization> getLatestValidMaterialization(final LayoutApiDescriptor layoutDescriptor,final Layout layout) {
    final long currentTime = System.currentTimeMillis();
    List<Materialization> result = AccelerationServiceImpl.COMPLETION_ORDERING
        .greatestOf(FluentIterable
            .from(accelerationService.getMaterializations(layoutDescriptor.getId()))
            .filter(new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable final Materialization materialization) {
                if (materialization.getState() != MaterializationState.DONE) {
                  return false;
                }
                if (materialization.getLayoutVersion() != layout.getVersion()) {
                  return false;
                }
                if (materialization.getExpiration() == null) {
                  return false;
                }
                long expiration = materialization.getExpiration();
                if (currentTime > expiration) {
                  return false;
                }
                return true;
              }
            }), 1);
    return Optional.fromNullable(Iterables.getLast(result, null));
  }

  /**
   * Writes materialization errors to given descriptor if any.
   *
   * Currently we only handle materialization query failures.
   */
  private void writeMaterializationFailuresIfAny(final AccelerationApiDescriptor descriptor) {
    for (LayoutApiDescriptor layout : getAllLayouts(descriptor)) {
      Optional<Materialization> materialization = getLatestMaterialization(layout.getId());

      if (!materialization.isPresent() || materialization.get().getState() != MaterializationState.FAILED) {
        continue;
      }

      final String jobId = Optional.fromNullable(materialization.get().getJob()).or(new JobDetails().setJobId("[unknown]")).getJobId();
      final MaterializatonFailure failure = Optional.fromNullable(materialization.get().getFailure()).or(MaterializatonFailure.getDefaultInstance());

      layout.setError(new ApiErrorDetails()
          .setCode(ApiErrorCode.MATERIALIZATION_FAILURE)
          .setMaterializationFailure(new MaterializationFailureDetails()
              .setJobId(jobId)
              .setMaterializationId(materialization.get().getId().getId()))
          .setMessage(failure.getMessage())
          .setStackTrace(failure.getStackTrace()));
    }
  }

  private Optional<Materialization> getLatestMaterialization(LayoutId layoutId) {
    Iterable<Materialization> materializations = accelerationService.getMaterializations(layoutId);
    return Optional.fromNullable(Iterables.getLast(materializations, null));
  }

  public static Iterable<LayoutApiDescriptor> getAllLayouts(final AccelerationApiDescriptor descriptor) {
    final LayoutContainerApiDescriptor aggContainer = Optional.fromNullable(descriptor.getAggregationLayouts()).or(EMPTY_CONTAINER);
    final LayoutContainerApiDescriptor rawContainer = Optional.fromNullable(descriptor.getRawLayouts()).or(EMPTY_CONTAINER);

    return Iterables.concat(AccelerationUtils.selfOrEmpty(aggContainer.getLayoutList()),
        AccelerationUtils.selfOrEmpty(rawContainer.getLayoutList()));
  }

  protected void checkForDatasetOutOfDate(final Optional<Acceleration> acceleration,
      final AccelerationDescriptor accelerationDescriptor) {
    if (acceleration.isPresent()) {
      checkForDatasetOutOfDate(acceleration.get(), accelerationDescriptor);
    }
  }

  /**
   * Marks acceleration as OUT_OF_DATE if
   *   1) schema is different for physical datasets
   *   2) schema or sql is different for virtual datasets. If schema is absent, then compare versions
   *
   * @param acceleration
   * @param accelerationDescriptor
   */
  protected void checkForDatasetOutOfDate(final Acceleration acceleration,
      final AccelerationDescriptor accelerationDescriptor) {
    if (acceleration.getContext() != null) {
      DatasetConfig acclerationDatasetConfig = acceleration.getContext().getDataset();
      if (acclerationDatasetConfig != null) {
        try {
          DatasetConfig datasetConfig = namespaceService.getDataset(new DatasetPath(acclerationDatasetConfig.getFullPathList()).toNamespaceKey());
          //if versions are same , then the acceleration cannot be OUT_OF_DATE
          if (!acclerationDatasetConfig.getVersion().equals(datasetConfig.getVersion())) {
            ByteString accelerationSchema = acclerationDatasetConfig.getRecordSchema();
            ByteString datasetSchema = datasetConfig.getRecordSchema();
            // if schema is absent or if dataset schema is different, then return OUT_OF_DATE
            if ((accelerationSchema == null || datasetSchema == null || !accelerationSchema.equals(datasetSchema))) {
              accelerationDescriptor.setState(AccelerationStateDescriptor.OUT_OF_DATE);
            } else if (acclerationDatasetConfig.getType() == DatasetType.VIRTUAL_DATASET &&
                !acclerationDatasetConfig.getVirtualDataset().getSql().equals(datasetConfig.getVirtualDataset().getSql())) {
              // if dataset is a virtual dataset, then compare the sql
              accelerationDescriptor.setState(AccelerationStateDescriptor.OUT_OF_DATE);
            }
          }
        } catch (NamespaceException e) {
          logger.info("Error while obtaining the datasource config", e);
        }
      }
    }
  }
}
