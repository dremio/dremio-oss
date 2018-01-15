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

import static com.dremio.dac.resource.ApiIntentMessageMapper.toLayoutId;

import java.util.List;

import javax.annotation.Nullable;

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
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.MaterializedLayoutState;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.base.Function;
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

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationResource.class);

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

    calculateAndSetSummaryInfo(acceleration, descriptor);

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

  private void calculateAndSetSummaryInfo(Acceleration acceleration, AccelerationApiDescriptor descriptor) {

    for (LayoutApiDescriptor layoutDescriptor : getAllLayouts(descriptor)) {
      Optional<MaterializedLayout> materializedLayout = accelerationService.getMaterializedLayout(toLayoutId(layoutDescriptor.getId()));
      Iterable<Materialization> materializations = AccelerationUtils.getAllMaterializations(materializedLayout);
      LayoutId layoutId = toLayoutId(layoutDescriptor.getId());
      layoutDescriptor.setState(materializedLayout
          .transform(new Function<MaterializedLayout, MaterializedLayoutState>() {
            @Override
            public MaterializedLayoutState apply(@Nullable final MaterializedLayout input) {
              return input.getState()!=null?input.getState():MaterializedLayoutState.ACTIVE;
            }
          })
          .or(MaterializedLayoutState.ACTIVE));
      final long currentTime = System.currentTimeMillis();
      Optional<Layout> layoutOpt = getLayout(acceleration, layoutId);
      if (!layoutOpt.isPresent()) {
        return;
      }
      final Layout layout = layoutOpt.get();
      Optional<Materialization> validMaterialization = getValidMaterialization(layout, materializations, currentTime);
      if (validMaterialization.isPresent()) {
        layoutDescriptor.setHasValidMaterialization(true);
        layoutDescriptor.setCurrentByteSize(validMaterialization.get().getMetrics().getFootprint());
      } else {
        layoutDescriptor.setHasValidMaterialization(false);
        layoutDescriptor.setCurrentByteSize(0L);
      }
      Optional<Materialization> latestMaterializationOpt = getLatestMaterialization(layoutId, materializations);
      if (latestMaterializationOpt.isPresent()) {
        layoutDescriptor.setLatestMaterializationState(latestMaterializationOpt.get().getState());
      }
      layoutDescriptor.setTotalByteSize(getTotalSize(layoutId, materializations));
    }
  }

  private Optional<Layout> getLayout(final Acceleration acceleration, LayoutId layoutId) {
    for (Layout layout : AccelerationUtils.getAllLayouts(acceleration)) {
      if (layout.getId().equals(layoutId)) {
        return Optional.fromNullable(layout);
      }
    }
    return Optional.absent();
  }

  public Optional<Materialization> getValidMaterialization(final Layout layout,
      final Iterable<Materialization> materializations, final long currentTime) {
    List<Materialization> result = AccelerationServiceImpl.COMPLETION_ORDERING
        .greatestOf(FluentIterable
            .from(materializations)
            .filter(new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable final Materialization materialization) {
                if (materialization.getState() != MaterializationState.DONE) {
                  return false;
                }
                if (isOutOfDate(layout, materialization)) {
                  return false;
                }
                if (hasExpired(materialization, currentTime)) {
                  return false;
                }
                return accelerationService.isMaterializationCached(materialization.getId());
              }
            }), 1);
    return Optional.fromNullable(Iterables.getLast(result, null));
  }

  private boolean isOutOfDate(final Layout layout, final Materialization materialization) {
    return !materialization.getLayoutVersion().equals(layout.getVersion());
  }

  private boolean hasExpired(final Materialization materialization, final long currentTime) {
    return( materialization.getExpiration() == null || currentTime > materialization.getExpiration());
  }

  private long getTotalSize(LayoutId id, Iterable<Materialization> materializations) {
    long totalSize = 0;
    for (Materialization materialization : materializations) {
      if (materialization.getState().equals(MaterializationState.DONE)) {
        totalSize += materialization.getMetrics().getFootprint();
      }
    }
    return totalSize;
  }

  /**
   * Writes materialization errors to given descriptor if any.
   *
   * Currently we only handle materialization query failures.
   */
  private void writeMaterializationFailuresIfAny(final AccelerationApiDescriptor descriptor) {
    for (LayoutApiDescriptor layout : getAllLayouts(descriptor)) {
      final LayoutId layoutId = toLayoutId(layout.getId());
      Iterable<Materialization> materializations = accelerationService.getMaterializations(layoutId);
      Optional<Materialization> materialization = getLatestMaterialization(layoutId, materializations);

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

  private Optional<Materialization> getLatestMaterialization(LayoutId layoutId, Iterable<Materialization> materializations) {
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
    if (accelerationDescriptor.getState() == AccelerationStateDescriptor.NEW ||
      accelerationDescriptor.getState() == AccelerationStateDescriptor.REQUESTED) {
      return;
    }
    if (acceleration.getContext() == null) {
      return;
    }
    DatasetConfig accelerationDatasetConfig = acceleration.getContext().getDataset();
    if (accelerationDatasetConfig == null) {
      return;
    }
    DatasetConfig datasetConfig = namespaceService.findDatasetByUUID(accelerationDatasetConfig.getId().getId());
    //if dataset is not present, then mark it as out of date.
    if (datasetConfig == null) {
      accelerationDescriptor.setState(AccelerationStateDescriptor.OUT_OF_DATE);
      return;
    }
    //if versions are same , then the acceleration cannot be OUT_OF_DATE
    if (accelerationDatasetConfig.getVersion().equals(datasetConfig.getVersion())) {
      return;
    }
    ByteString accelerationSchema = accelerationDatasetConfig.getRecordSchema();
    ByteString datasetSchema = datasetConfig.getRecordSchema();
    // if schema is absent or if dataset schema is different, then return OUT_OF_DATE
    if ((accelerationSchema == null || datasetSchema == null || !accelerationSchema.equals(datasetSchema))) {
      accelerationDescriptor.setState(AccelerationStateDescriptor.OUT_OF_DATE);
    } else if (accelerationDatasetConfig.getType() == DatasetType.VIRTUAL_DATASET &&
        !accelerationDatasetConfig.getVirtualDataset().getSql().equals(datasetConfig.getVirtualDataset().getSql())) {
      // if dataset is a virtual dataset, then compare the sql
      accelerationDescriptor.setState(AccelerationStateDescriptor.OUT_OF_DATE);
    }
  }
}
