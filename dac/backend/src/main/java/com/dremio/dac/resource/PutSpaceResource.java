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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.UserNotFoundException;

/**
 * Rest resource for spaces.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/space/{spaceName}")
public class PutSpaceResource {
  private final NamespaceService namespaceService;
  private final SpacePath spacePath;

  @Inject
  public PutSpaceResource(
      NamespaceService namespaceService,
      @PathParam("spaceName") SpaceName spaceName) {
    this.namespaceService = namespaceService;
    this.spacePath = new SpacePath(spaceName);
  }

  public static SpaceConfig addOrUpdateSpace(NamespaceService service,
                                             SpacePath spacePath, Space space)
    throws NamespaceException, UserNotFoundException {

    SpaceConfig spaceConfig = new SpaceConfig()
      .setId(space.getId() != null ? new EntityId(space.getId()) : null)
      .setName(space.getName())
      .setDescription(space.getDescription())
      .setTag(space.getVersion());

    service.addOrUpdateSpace(spacePath.toNamespaceKey(), spaceConfig);
    return service.getSpace(spacePath.toNamespaceKey());
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Space putSpace(Space space) throws NamespaceException, UserNotFoundException {
    SpaceConfig spaceConfig = PutSpaceResource.addOrUpdateSpace(namespaceService, spacePath, space);
    return Space.newInstance(spaceConfig, null, namespaceService.getDatasetCount(spacePath.toNamespaceKey(), BoundedDatasetCount.SEARCH_TIME_LIMIT_MS, BoundedDatasetCount.COUNT_LIMIT_TO_STOP_SEARCH).getCount());
  }
}
