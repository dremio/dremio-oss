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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.service.accelerator.AccelerationService;

/**
 * API for setting low-level development options. Not meant to be a permanent API.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/development_options")
public class DevelopmentOptionsResource {

  private final AccelerationService accelerationService;

  @Inject
  public DevelopmentOptionsResource(final AccelerationService accelerationService) {
    this.accelerationService = accelerationService;
  }

  @GET
  @Path("/acceleration/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public String isGlobalAccelerationEnabled() {
    return Boolean.toString(accelerationService.developerService().isQueryAccelerationEnabled());
  }

  @PUT
  @Path("/acceleration/enabled")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String setAccelerationEnabled(/* Body */String body) {
    boolean enabled = Boolean.valueOf(body);
    accelerationService.developerService().enableQueryAcceleration(enabled);

    return body;
  }

  @GET
  @Path("/acceleration/graph")
  public String getDependencyGraph() {
    return accelerationService.developerService().getDependencyGraph().toString();
  }

  @POST
  @Path("/acceleration/build")
  public void triggerAccelerationNewBuild() {
    accelerationService.developerService().triggerAccelerationBuild();
  }

  @POST
  @Path("/acceleration/compact")
  public void triggerAccelerationCompaction() {
    accelerationService.developerService().triggerCompaction();
  }

  @POST
  @Path("/acceleration/clearall")
  public void clearMaterializations() {
    accelerationService.developerService().clearAllAccelerations();
  }
}
