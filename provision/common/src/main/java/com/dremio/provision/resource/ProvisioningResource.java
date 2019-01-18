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
package com.dremio.provision.resource;

import static com.dremio.provision.service.ProvisioningServiceImpl.DEFAULT_HEAP_MEMORY_MB;
import static com.dremio.provision.service.ProvisioningServiceImpl.LARGE_SYSTEMS_DEFAULT_HEAP_MEMORY_MB;
import static com.dremio.provision.service.ProvisioningServiceImpl.LARGE_SYSTEMS_MIN_MEMORY_MB;
import static com.dremio.provision.service.ProvisioningServiceImpl.MIN_MEMORY_REQUIRED_MB;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterCreateRequest;
import com.dremio.provision.ClusterDesiredState;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterModifyRequest;
import com.dremio.provision.ClusterResponse;
import com.dremio.provision.ClusterResponses;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DynamicConfig;
import com.dremio.provision.Property;
import com.dremio.provision.PropertyType;
import com.dremio.provision.ResizeClusterRequest;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.provision.service.ProvisioningService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;


/**
 * Resource to deal with Cluster Provisioning
 */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/provision")
public class ProvisioningResource {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProvisioningResource.class);


  private final ProvisioningService service;
  //private boolean isMaster;

  @Inject
  public ProvisioningResource(ProvisioningService service) {
    this.service = service;
  }

  @PUT
  @Path("/cluster/{id}/dynamicConfig")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterResponse resizeCluster(@PathParam("id") final String id,
                                       ResizeClusterRequest clusterResizeRequest) throws Exception {

    final ClusterId clusterId = new ClusterId(id);
    final ClusterEnriched cluster = service.resizeCluster(clusterId, clusterResizeRequest.getContainerCount().intValue());
    return toClusterResponse(cluster);
  }

  @POST
  @Path("/cluster")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterResponse createCluster(ClusterCreateRequest clusterCreateRequest) throws Exception {

    ClusterConfig clusterConfig = getClusterConfig(clusterCreateRequest);
    ClusterEnriched cluster = service.createCluster(clusterConfig);
    return toClusterResponse(cluster);
   }

  public ClusterConfig getClusterConfig(ClusterCreateRequest clusterCreateRequest) {
    Preconditions.checkNotNull(clusterCreateRequest.getMemoryMB());
    Preconditions.checkNotNull(clusterCreateRequest.getDistroType());
    Preconditions.checkNotNull(clusterCreateRequest.getIsSecure());

    if(clusterCreateRequest.getMemoryMB() < MIN_MEMORY_REQUIRED_MB) {
      throw new IllegalArgumentException("Minimum memory required should be greater or equal than: " +
        MIN_MEMORY_REQUIRED_MB + "MB");
    }

    int onHeap = clusterCreateRequest.getMemoryMB() < LARGE_SYSTEMS_MIN_MEMORY_MB
                                                    ? DEFAULT_HEAP_MEMORY_MB
                                                    : LARGE_SYSTEMS_DEFAULT_HEAP_MEMORY_MB;
    List<Property> properties = Optional.fromNullable(clusterCreateRequest.getSubPropertyList()).or(new ArrayList<Property>());
    for (Property prop : properties) {
      if (ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY.equalsIgnoreCase(prop.getKey())) {
        String onHeapStr = prop.getValue();
        try {
          onHeap = Integer.valueOf(onHeapStr);
        } catch (NumberFormatException nfe) {
          logger.warn("Heap memory specified is not numeric, using default {}", onHeap);
        }
        break;
      }
    }
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setName(clusterCreateRequest.getName());
    clusterConfig.setClusterType(clusterCreateRequest.getClusterType());
    clusterConfig.setClusterSpec(new ClusterSpec()
      .setMemoryMBOffHeap(clusterCreateRequest.getMemoryMB() - onHeap)
      .setMemoryMBOnHeap(onHeap)
      .setContainerCount(clusterCreateRequest.getDynamicConfig().getContainerCount())
      .setVirtualCoreCount(clusterCreateRequest.getVirtualCoreCount())
      .setQueue(clusterCreateRequest.getQueue()));
    clusterConfig.setDistroType(clusterCreateRequest.getDistroType());
    clusterConfig.setIsSecure(clusterCreateRequest.getIsSecure());
    clusterConfig.setSubPropertyList(clusterCreateRequest.getSubPropertyList());
    return clusterConfig;
  }

  @PUT
  @Path("/cluster/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterResponse modifyCluster(@PathParam("id") final String id, ClusterModifyRequest clusterModifyRequest) throws
    ProvisioningHandlingException {

    final ClusterId clusterId = new ClusterId(id);

    final ClusterConfig clusterConfig = toClusterConfig(clusterModifyRequest);

    ClusterEnriched cluster = service.modifyCluster(clusterId, toState(clusterModifyRequest.getDesiredState()),
      clusterConfig);
    return toClusterResponse(cluster);
  }


  @DELETE
  @Path("/cluster/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteCluster(@PathParam("id") final String id) throws Exception {
    service.deleteCluster(new ClusterId(id));
    // TODO set response to OK
    return;
  }

  @GET
  @Path("/cluster/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterResponse getClusterInfo(@PathParam("id") final String id) throws Exception {
    ClusterEnriched cluster = service.getClusterInfo(new ClusterId(id));
    return toClusterResponse(cluster);
  }

  @GET
  @Path("/clusters")
  @Produces(MediaType.APPLICATION_JSON)
  public ClusterResponses getClustersInfo(@DefaultValue("") @QueryParam("type") final String type) throws Exception {
    final Iterable<ClusterEnriched> clusters;
    if (type.isEmpty()) {
      // use all the types
      clusters = service.getClustersInfo();
    } else {
      ClusterType clusterType = ClusterType.valueOf(type.toUpperCase());
      clusters = service.getClusterInfoByType(clusterType);
    }

    ClusterResponses responses = new ClusterResponses();
    responses.setClusterList((FluentIterable.from(clusters).transform(new Function<ClusterEnriched, ClusterResponse>() {
      @Override
      public ClusterResponse apply(ClusterEnriched input) {
        return toClusterResponse(input);
      }
    }).toList()));
    return responses;
  }

  private static ClusterResponse toClusterResponse(ClusterEnriched clusterEnriched) {
    Cluster cluster = clusterEnriched.getCluster();
    ClusterResponse response = new ClusterResponse();
    response.setCurrentState(cluster.getState());
    if (cluster.getError() != null) {
      response.setError(cluster.getError());
    }
    response.setId(cluster.getId().getId());
    response.setTag(cluster.getClusterConfig().getTag());
    response.setClusterType(cluster.getClusterConfig().getClusterType());
    response.setName(cluster.getClusterConfig().getName());
    response.setDynamicConfig(new DynamicConfig()
      .setContainerCount(cluster.getClusterConfig().getClusterSpec().getContainerCount()));
    response.setMemoryMB(cluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap() +
      cluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap());
    response.setVirtualCoreCount(cluster.getClusterConfig().getClusterSpec().getVirtualCoreCount());
    response.setQueue(cluster.getClusterConfig().getClusterSpec().getQueue());
    // take care of the property types
    final List<Property> properties = cluster.getClusterConfig().getSubPropertyList();
    for (Property prop : properties) {
      if (prop.getType() == null) {
        if (prop.getKey().startsWith("-X")) {
          prop.setType(PropertyType.SYSTEM_PROP);
        } else {
          prop.setType(PropertyType.JAVA_PROP);
        }
      }
    }
    response.setSubPropertyList(cluster.getClusterConfig().getSubPropertyList());
    response.setContainers(clusterEnriched.getRunTimeInfo());
    response.setDesiredState(toDesiredState((cluster.getDesiredState() != null) ? cluster.getDesiredState() :
        cluster.getState()));
    response.setDistroType(cluster.getClusterConfig().getDistroType());
    response.setIsSecure(cluster.getClusterConfig().getIsSecure());
    return response;
  }

  @VisibleForTesting
  public ClusterConfig toClusterConfig(final ClusterModifyRequest clusterModifyRequest) {
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setName(Optional.fromNullable(clusterModifyRequest.getName()).orNull());
    clusterConfig.setClusterType(clusterModifyRequest.getClusterType());
    clusterConfig.setTag(clusterModifyRequest.getTag());
    clusterConfig.setSubPropertyList(clusterModifyRequest.getSubPropertyList());
    ClusterSpec clusterSpec = new ClusterSpec();

    int onHeap = 0;
    if (clusterConfig.getSubPropertyList() != null) {
      for (Property prop : clusterConfig.getSubPropertyList()) {
        if (!ProvisioningService.YARN_HEAP_SIZE_MB_PROPERTY.equalsIgnoreCase(prop.getKey())) {
          continue;
        }
        String onHeapStr = prop.getValue();
        try {
          onHeap = Integer.valueOf(onHeapStr);
        } catch (NumberFormatException nfe) {
          throw new IllegalArgumentException("Heap memory specified is not numeric: " + onHeapStr);
        }
        clusterSpec.setMemoryMBOnHeap(onHeap);
        break;
      }
    }
    if (clusterModifyRequest.getMemoryMB() != null) {
      // total memory was passed in. if we don't know on-heap - adjust it later on
      clusterSpec.setMemoryMBOffHeap(clusterModifyRequest.getMemoryMB() - onHeap);
    }

    clusterSpec.setVirtualCoreCount(Optional.fromNullable(clusterModifyRequest.getVirtualCoreCount()).orNull());
    DynamicConfig requestConfig = Optional.fromNullable(clusterModifyRequest.getDynamicConfig()).or(new DynamicConfig());
    clusterSpec.setContainerCount(Optional.fromNullable(requestConfig.getContainerCount()).orNull());
    clusterSpec.setQueue(Optional.fromNullable(clusterModifyRequest.getQueue()).orNull());

    clusterConfig.setClusterSpec(clusterSpec);
    return clusterConfig;
  }

  private static ClusterDesiredState toDesiredState(ClusterState state) {
    // at this point we have few "desired" states:
    // DELETED - while cluster is stopping
    // RUNNING - this state indicates that cluster is restarting and needs to get into RUNNING state
    switch (state) {
      case RUNNING:
      case STARTING:
      case CREATED:
        return ClusterDesiredState.RUNNING;
      case DELETED:
        return ClusterDesiredState.DELETED;
      case STOPPED:
      case FAILED:
      case STOPPING:
        return ClusterDesiredState.STOPPED;
      default:
        // should not be any more "desired" states at this point
        logger.error("unexpected \"desired\" state: " + state);
        return null;
    }
  }

  private static ClusterState toState(ClusterDesiredState state) {
    if (state == null) {
      // no desired state was specified. BE will figure it out
      return null;
    }
    // can throw IllegalArgumentException
    return ClusterState.valueOf(state.name());
  }
}
