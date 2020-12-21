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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.info.VersionInfo;
import com.dremio.dac.util.InformationUtil;
import com.dremio.provision.service.ProvisioningServiceImpl;

/**
 * Information API resource.
 */
@APIResource
@Secured
@RolesAllowed({"user", "admin"})
@Path("/info")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class InformationResource {

  @GET
  public VersionInfo getNodeInfo() {
    return InformationUtil.getVersionInfo(ProvisioningServiceImpl.getType());
  }
}
