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

import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationInfoApiDescriptor;
import com.dremio.dac.service.errors.AccelerationNotFoundException;
import com.dremio.dac.service.errors.ResourceExistsException;
import com.dremio.service.accelerator.AccelerationMapper;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationContextDescriptor;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMetrics;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationRequest;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationType;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;


/**
 * Resource to access acceleration service
 */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/accelerations")
public class AccelerationResource extends BaseAccelerationResource {

  public static final String DIM_MEASURE_REQUIRED_BASIC =
    "Dimensions and measures must both be specified for basic aggregate acceleration.";

  private final ApiIntentMessageMapper mapper = new ApiIntentMessageMapper();
  private final AccelerationService accelerationService;
  private final NamespaceService namespaceService;

  @Context
  private SecurityContext securityContext;

  @Inject
  public AccelerationResource(final AccelerationService accelerationService, final NamespaceService namespaceService) {
    super(accelerationService, namespaceService);
    this.accelerationService = accelerationService;
    this.namespaceService = namespaceService;
  }

  // API

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AccelerationApiDescriptor createAcceleration(final DatasetPath path) throws NamespaceException, ResourceExistsException {
    final Optional<AccelerationEntry> entry = accelerationService.getAccelerationEntryByDataset(path.toNamespaceKey());
    if (entry.isPresent()) {
      throw new ResourceExistsException(String.format("acceleration exists at %s", path));
    }

    final AccelerationEntry newEntry = createDatasetAcceleration(namespaceService, accelerationService, path, AccelerationState.NEW_ADMIN);
    return mapper.toApiMessage(newEntry.getDescriptor(), path.toPathList());
  }


  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{id}")
  public AccelerationApiDescriptor getAcceleration(@PathParam("id") final AccelerationId id) throws AccelerationNotFoundException, NamespaceException {
    final String message = String.format("unable to find acceleration at %s", id.getId());
    final AccelerationEntry accelerationEntry = getOrFailChecked(accelerationService.getAccelerationEntryById(id), message);
    final Optional<Acceleration> acceleration = accelerationService.getAccelerationById(id);
    DatasetConfig ds = namespaceService.findDatasetByUUID(id.getId());
    checkForDatasetOutOfDate(acceleration, accelerationEntry.getDescriptor());
    return augment(mapper.toApiMessage(accelerationEntry.getDescriptor(), ds.getFullPathList()));
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{id}")
  public AccelerationApiDescriptor save(
      @PathParam("id") final AccelerationId id,
      final AccelerationDescriptor newDescriptor) throws AccelerationNotFoundException {
    Preconditions.checkArgument(id != null, "acceleration id is required");
    Preconditions.checkArgument(id.equals(newDescriptor.getId()), "id mismatch");
    newDescriptor.getRawLayouts().setType(LayoutType.RAW);
    newDescriptor.getAggregationLayouts().setType(LayoutType.AGGREGATION);

    final AccelerationEntry entry = getOrFailChecked(accelerationService.getAccelerationEntryById(id), id.getId());
    final AccelerationDescriptor curDescriptor = entry.getDescriptor();

    // do not allow user to modify state externally
    newDescriptor.setState(curDescriptor.getState());

    // do not allow user to modify context except logical aggregation
    curDescriptor.getContext().setLogicalAggregation(newDescriptor.getContext().getLogicalAggregation());
    // set effective context on descriptor
    newDescriptor.setContext(curDescriptor.getContext());
    newDescriptor.setVersion(curDescriptor.getVersion());

    entry.setDescriptor(newDescriptor);

    AccelerationUtils.normalize(newDescriptor);
    Validators.validate(newDescriptor);

    final AccelerationEntry newEntryOpt = accelerationService.update(entry);
    DatasetConfig ds = namespaceService.findDatasetByUUID(id.getId());
    return augment(mapper.toApiMessage(newEntryOpt.getDescriptor(), ds.getFullPathList()));
  }

  @DELETE
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{id}")
  public void removeAcceleration(@PathParam("id") final AccelerationId id) {
    accelerationService.remove(id);
  }

  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/job/{id}")
  public AccelerationInfoApiDescriptor getJobAccelerationInfo(@PathParam("id") final JobId id) throws AccelerationNotFoundException, NamespaceException {
    Preconditions.checkArgument(id != null, "job id is required");

    final String message = String.format("unable to find acceleration at %s", id.getId());
    final AccelerationEntry acceleration = getOrFailChecked(accelerationService.getAccelerationEntryByJob(id), message);
    return getAccelerationInfo(acceleration);
  }

  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/dataset/{path}")
  public AccelerationInfoApiDescriptor getDatasetAccelerationInfo(@PathParam("path") final DatasetPath path) throws AccelerationNotFoundException, NamespaceException {
    Preconditions.checkArgument(path != null, "path is required");
    final String message = String.format("unable to find acceleration at %s", path.toPathString());
    final AccelerationEntry holder = getOrFailChecked(accelerationService.getAccelerationEntryByDataset(path.toNamespaceKey()), message);
    final Optional<Acceleration> acceleration = accelerationService.getAccelerationById(holder.getDescriptor().getId());
    checkForDatasetOutOfDate(acceleration, holder.getDescriptor());
    return getAccelerationInfo(holder);
  }

  // helper methods

  public static AccelerationEntry createDatasetAcceleration(final NamespaceService namespaceService, AccelerationService accelerationService, final DatasetPath path, final AccelerationState state) throws NamespaceException {
    final DatasetConfig config = namespaceService.getDataset(path.toNamespaceKey());

    final Acceleration acceleration = new Acceleration()
        .setId(new AccelerationId(config.getId().getId()))
        .setType(AccelerationType.DATASET)
        .setState(state)
        .setContext(new AccelerationContext()
            .setDatasetSchema(new RowType())
            .setLogicalAggregation(new LogicalAggregation())
            .setDataset(config)
        )
        .setAggregationLayouts(new LayoutContainer().setType(LayoutType.AGGREGATION))
        .setRawLayouts(new LayoutContainer().setType(LayoutType.RAW))
        .setMetrics(new AccelerationMetrics())
        .setPipeline(new AccelerationPipeline());

    return accelerationService.create(acceleration);
  }

  protected AccelerationInfoApiDescriptor getAccelerationInfo(final AccelerationEntry holder) {
    final List<AccelerationRequest> requests = AccelerationUtils.selfOrEmpty(holder.getRequestList());
    final String username = securityContext.getUserPrincipal().getName();
    final Optional<AccelerationRequest> selfRequest = FluentIterable.from(requests)
        .firstMatch(new Predicate<AccelerationRequest>() {
          @Override
          public boolean apply(@Nullable final AccelerationRequest input) {
            return input.getUsername().equals(username);
          }
        });

    final boolean selfRequested = selfRequest.isPresent();

    final AccelerationDescriptor descriptor = holder.getDescriptor();

    return new AccelerationInfoApiDescriptor()
        .setId(descriptor.getId())
        .setState(mapper.toApiMessage(descriptor.getState()))
        .setAggregationEnabled(descriptor.getAggregationLayouts().getEnabled())
        .setRawAccelerationEnabled(descriptor.getRawLayouts().getEnabled())
        .setSelfRequested(selfRequested)
        .setTotalRequests(requests.size());
  }



  // utility classes

  private static final class Validators {

    public static void validate(final AccelerationDescriptor descriptor) {
      Preconditions.checkArgument(descriptor != null, "descriptor is required");
      Preconditions.checkArgument(descriptor.getId() != null, "descriptor.id is required");
      Preconditions.checkArgument(descriptor.getId().getId() != null, "descriptor.id is required");
      Preconditions.checkArgument(descriptor.getVersion() != null, "descriptor.version is required");
      final AccelerationMode mode = descriptor.getMode();
      Preconditions.checkArgument(mode != null, "descriptor.mode is required");

      final LayoutContainerDescriptor aggLayoutContainer = descriptor.getAggregationLayouts();
      final LayoutContainerDescriptor rawLayouts = descriptor.getRawLayouts();

      if (mode == AccelerationMode.AUTO) {
        final AccelerationContextDescriptor context = descriptor.getContext();
        Preconditions.checkArgument(context != null, "descriptor.context is required");
        final LogicalAggregationDescriptor logical = context.getLogicalAggregation();

        // writing condition like this prevents NPE on prop not set
        if (Boolean.TRUE.equals(aggLayoutContainer.getEnabled())) {
          Preconditions.checkArgument(logical != null, "descriptor.context.logicalAggregation is required");
          Preconditions.checkArgument(!AccelerationUtils.selfOrEmpty(logical.getDimensionList()).isEmpty()
                                      && !AccelerationUtils.selfOrEmpty(logical.getMeasureList()).isEmpty(),
                                      DIM_MEASURE_REQUIRED_BASIC);
        }
      } else if (mode == AccelerationMode.MANUAL) {
        // writing condition like this prevents NPE on prop not set
        if (Boolean.TRUE.equals(aggLayoutContainer.getEnabled())) {
          // validate aggregation layouts
          Preconditions.checkArgument(aggLayoutContainer != null, "descriptor.aggregationLayouts is required");

          final List<LayoutDescriptor> aggLayoutList = AccelerationUtils.selfOrEmpty(aggLayoutContainer.getLayoutList());
          Preconditions.checkArgument(!aggLayoutList.isEmpty(), "descriptor.aggregationLayouts.layoutList is required");
          for (final LayoutDescriptor layoutDescriptor : aggLayoutList) {
            validateAggregationLayout(layoutDescriptor);
          }
        }

        // writing condition like this prevents NPE on prop not set
        if (Boolean.TRUE.equals(rawLayouts.getEnabled())) {
          // validate raw layouts
          Preconditions.checkArgument(rawLayouts != null, "descriptor.rawLayouts is required");
          Preconditions.checkArgument(rawLayouts.getEnabled() != null, "descriptor.rawLayouts.enabled is required");

          final List<LayoutDescriptor> rawLayoutList = AccelerationUtils.selfOrEmpty(rawLayouts.getLayoutList());
          Preconditions.checkArgument(!rawLayoutList.isEmpty(), "descriptor.rawLayouts.layoutList is required");
          for (final LayoutDescriptor layoutDescriptor : rawLayoutList) {
            validateRawLayout(layoutDescriptor);
          }
        }
      } else {
        throw new IllegalArgumentException(String.format("unsupported mode: %s", mode));
      }

      // validate individual layout names
      AccelerationMapper.create(descriptor.getContext().getDatasetSchema())
          .toLogicalAggregation(descriptor.getContext().getLogicalAggregation());
    }

    public static void validateAggregationLayout(final LayoutDescriptor layoutDescriptor) {
      final LayoutDetailsDescriptor details = layoutDescriptor.getDetails();
      Preconditions.checkArgument(details != null, "descriptor.aggregationLayouts -> details is required");
      Preconditions.checkArgument(!AccelerationUtils.selfOrEmpty(details.getDimensionFieldList()).isEmpty(), "descriptor.aggregationLayouts -> details.dimensionFieldList is required");
    }

    public static void validateRawLayout(final LayoutDescriptor layoutDescriptor) {
      final LayoutDetailsDescriptor details = layoutDescriptor.getDetails();
      Preconditions.checkArgument(details != null, "descriptor.rawLayouts -> details is required");
      Preconditions.checkArgument(!AccelerationUtils.selfOrEmpty(details.getDisplayFieldList()).isEmpty(), "descriptor.rawLayouts -> details.displayFieldList is required");
    }

  }

  private static <T> T getOrFailChecked(final Optional<T> entry, final String message) throws AccelerationNotFoundException {
    if (entry.isPresent()) {
      return entry.get();
    }

    throw new AccelerationNotFoundException(message);
  }
}
