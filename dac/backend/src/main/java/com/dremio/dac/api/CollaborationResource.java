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

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.collaboration.Wiki;
import com.dremio.service.namespace.NamespaceException;
import java.util.Optional;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

/** Collaboration API resource. */
@APIResource
@Secured
@RolesAllowed({"user", "admin"})
@Path("/catalog/{id}/collaboration")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class CollaborationResource {
  private final CollaborationHelper collaborationHelper;

  @Inject
  public CollaborationResource(CollaborationHelper collaborationHelper) {
    this.collaborationHelper = collaborationHelper;
  }

  @GET
  @Path("/tag")
  public Tags getTagsForEntity(@PathParam("id") String id) throws NamespaceException {
    Optional<Tags> tags = collaborationHelper.getTags(id);
    return tags.orElseGet(() -> new Tags(null, null));
  }

  @POST
  @Path("/tag")
  public Tags setTagsForEntity(@PathParam("id") String id, Tags tags) throws NamespaceException {
    collaborationHelper.setTags(id, tags);

    return getTagsForEntity(id);
  }

  @GET
  @Path("/wiki")
  public Wiki getWikiForEntity(@PathParam("id") String id) throws NamespaceException {
    Optional<Wiki> wiki = collaborationHelper.getWiki(id);
    return wiki.orElseGet(() -> new Wiki("", null));
  }

  @POST
  @Path("/wiki")
  public Wiki setWikiForEntity(@PathParam("id") String id, Wiki wiki) throws NamespaceException {
    collaborationHelper.setWiki(id, wiki);

    return getWikiForEntity(id);
  }
}
