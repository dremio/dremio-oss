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

import static com.dremio.exec.ExecConstants.NESSIE_SOURCE_API;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV2;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.errors.NessieSourceNotValidException;
import com.dremio.dac.service.errors.NessieSourceResourceException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NessieApiProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.services.nessie.proxy.ProxyV2TreeResource;
import com.google.common.base.Preconditions;

/**
 * Resource for providing APIs for Nessie As a Source.
 */
@Secured
@RolesAllowed({"admin", "user"})
@Path("/v2/source/{sourceName}/trees")
@Options
public class NessieSourceResource {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieSourceResource.class);
  private CatalogService catalogService;
  private OptionManager optionManager;

  @Inject
  public NessieSourceResource(
    CatalogService catalogService,
    OptionManager optionManager
  ) {
    this.catalogService = catalogService;
    this.optionManager = optionManager;
  };

  @Path("/")
  public ProxyV2TreeResource handle(@PathParam("sourceName") String sourceName) {
    if (optionManager.getOption(NESSIE_SOURCE_API)) {
      NessieApiProvider provider;
      try {
        provider = catalogService.getSource(sourceName);
      } catch (UserException namespaceNotFoundException) {
        logger.error(String.format("Cannot find source: %s", sourceName));
        throw new SourceNotFoundException(sourceName, namespaceNotFoundException);
      } catch (ClassCastException classCastException) {
        logger.error(String.format("%s is not versioned source", sourceName));
        throw new NessieSourceNotValidException(classCastException, String.format("%s is not versioned source", sourceName));
      } catch (Exception exception) {
        logger.error("Unexpected Error");
        throw new NessieSourceResourceException(exception, "Unexpected Error", BAD_REQUEST);
      }
      NessieApi nessieApi = provider.getNessieApi();
      Preconditions.checkArgument(nessieApi instanceof NessieApiV2, "nessieApi provided by NessieApiProvider is not V2. V2 is required.");
      return getTreeResource(nessieApi);
    } else {
      logger.error(String.format("Using nessie-as-a-source is disabled. The support key '%s' must be enabled.", NESSIE_SOURCE_API.getOptionName()));
      throw new NotFoundException(String.format("Using nessie-as-a-source is disabled. The support key '%s' must be enabled.", NESSIE_SOURCE_API.getOptionName()));
    }
  }

  protected ProxyV2TreeResource getTreeResource(NessieApi nessieApi) {
    return new V2TreeResource((NessieApiV2) nessieApi);
  }
}
