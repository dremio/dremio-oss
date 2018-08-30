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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.collaboration.CollaborationService;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.collaboration.Wiki;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Optional;

/**
 * Collaboration API resource.
 */
@Secured
@RolesAllowed({"user", "admin"})
@Path("/catalog/{id}/collaboration")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class CollaborationResource {
  private final CollaborationService collaborationService;

  @Inject
  public CollaborationResource(CollaborationService collaborationService) {
    this.collaborationService = collaborationService;
  }

  @GET
  @Path("/tag")
  public Tags getTagsForEntity(@PathParam("id") String id) throws NamespaceException {
    Optional<Tags> tags = collaborationService.getTags(id);

    if (!tags.isPresent()) {
      throw new NotFoundException(String.format("Entity [%s] does not have any tags set.", id));
    }

    return tags.get();
  }

  @POST
  @Path("/tag")
  public void setTagsForEntity(@PathParam("id") String id, Tags tags) throws NamespaceException {
    collaborationService.setTags(id, tags);
  }

  @GET
  @Path("/wiki")
  public Wiki getWikiForEntity(@PathParam("id") String id) throws NamespaceException {
    Optional<Wiki> wiki = collaborationService.getWiki(id);

    if (!wiki.isPresent()) {
      throw new NotFoundException(String.format("Entity [%s] does not have a wiki set.", id));
    }

    return wiki.get();
  }

  @POST
  @Path("/wiki")
  public void setWikiForEntity(@PathParam("id") String id, Wiki wiki) throws NamespaceException {
    collaborationService.setWiki(id, wiki);
  }
}
