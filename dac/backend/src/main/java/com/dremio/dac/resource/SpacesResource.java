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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.model.spaces.Spaces;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Resource for information about spaces.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/spaces")
public class SpacesResource {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpacesResource.class);

  private final NamespaceService namespaceService;

  @Inject
  public SpacesResource(NamespaceService namespaceService) {
    this.namespaceService = namespaceService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Spaces getSpaces() throws Exception {
    final Spaces spaces = new Spaces();
    for (SpaceConfig spaceConfig : namespaceService.getSpaces()) {
      int datasetCount = 0;

      try {
        datasetCount = namespaceService.getAllDatasetsCount(new SpacePath(spaceConfig.getName()).toNamespaceKey());
      } catch (IllegalArgumentException e) {
        logger.warn("Could not load dataset count for {} because it has a invalid name: {}", spaceConfig.getName(), e.getMessage());
      } catch (NamespaceException e) {
        logger.warn("Could not load dataset count for {}: {}", spaceConfig.getName(), e.getMessage());
      }

      try {
        // we catch exceptions here so the user can still see their other Spaces
        spaces.add(newSpace(spaceConfig, datasetCount));
      } catch (NamespaceNotFoundException e) {
        logger.warn("Skipping Space {} because namespace not found: {}", spaceConfig.getName(), e.getMessage());
      }
    }
    return spaces;
  }

  protected Space newSpace(SpaceConfig spaceConfig, int datasetCount) throws Exception {
    return Space.newInstance(spaceConfig, null, datasetCount);
  }
}
