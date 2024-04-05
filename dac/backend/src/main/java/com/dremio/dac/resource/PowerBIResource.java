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
package com.dremio.dac.resource;

import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_PBIDS;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

/** Resource to create PBIDS files for a given dataset */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/powerbi/{path: .*}")
@Options
public class PowerBIResource extends BaseBIToolResource {
  // Special option for enabling the Power BI PBIDS endpoint.
  public static final TypeValidators.BooleanValidator CLIENT_TOOLS_POWERBI =
      new TypeValidators.BooleanValidator("client.tools.powerbi", true);

  @Inject
  public PowerBIResource(
      ProjectOptionManager optionManager,
      DatasetCatalog datasetCatalog,
      @PathParam("path") String path,
      @QueryParam("refType") String refType,
      @QueryParam("refValue") String refValue) {
    super(optionManager, datasetCatalog, path, refType, refValue);
  }

  /**
   * Returns a PBIDS file for the dataset
   *
   * @return A PBIDS file that lets the caller load the given dataset in Power BI.
   */
  @GET
  @Produces(APPLICATION_PBIDS)
  public Response get(@HeaderParam(HttpHeaders.HOST) String host) throws DatasetNotFoundException {
    return getWithHostHelper(host);
  }

  @Override
  protected TypeValidators.BooleanValidator getClientToolOption() {
    return CLIENT_TOOLS_POWERBI;
  }
}
