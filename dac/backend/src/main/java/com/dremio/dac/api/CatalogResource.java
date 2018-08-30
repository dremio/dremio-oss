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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.PathSegment;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Optional;

/**
 * Catalog API resource.
 */
@APIResource
@Secured
@RolesAllowed({"user", "admin"})
@Path("/catalog")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class CatalogResource {
  private final CatalogServiceHelper catalogServiceHelper;

  @Inject
  public CatalogResource(CatalogServiceHelper catalogServiceHelper) {
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @GET
  public ResponseList<CatalogItem> listTopLevelCatalog() {
    ResponseList<CatalogItem> catalogItems = new ResponseList<>(catalogServiceHelper.getTopLevelCatalogItems());

    return catalogItems;
  }

  @GET
  @Path("/{id}")
  public CatalogEntity getCatalogItem(@PathParam("id") String id) throws NamespaceException {
    Optional<CatalogEntity> entity = catalogServiceHelper.getCatalogEntityById(id);

    if (!entity.isPresent()) {
      throw new NotFoundException(String.format("Could not find entity with id [%s]", id));
    }

    return entity.get();
  }

  @POST
  public CatalogEntity createCatalogItem(CatalogEntity entity) throws NamespaceException, BadRequestException {
    try {
      return catalogServiceHelper.createCatalogItem(entity);
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    } catch (ExecutionSetupException e) {
      throw new InternalServerErrorException(e);
    }
  }

  @POST
  @Path("/{id}")
  public Dataset promoteToDataset(Dataset dataset, @PathParam("id") String id) throws NamespaceException, BadRequestException {
    try {
      return catalogServiceHelper.promoteToDataset(id, dataset);
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @PUT
  @Path("/{id}")
  public CatalogEntity updateCatalogItem(CatalogEntity entity, @PathParam("id") String id) throws NamespaceException, BadRequestException {
    try {
      return catalogServiceHelper.updateCatalogItem(entity, id);
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } catch (ExecutionSetupException e) {
      throw new InternalServerErrorException(e);
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @DELETE
  @Path("/{id}")
  public void deleteCatalogItem(@PathParam("id") String id, @QueryParam("tag") String tag) throws NamespaceException, BadRequestException {
    try {
      catalogServiceHelper.deleteCatalogItem(id, tag);
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @POST
  @Path("/{id}/refresh")
  public void refreshCatalogItem(@PathParam("id") String id) {
    try {
      catalogServiceHelper.refreshCatalogItem(id);
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @GET
  @Path("/by-path/{segment:.*}")
  public CatalogEntity getCatalogItemByPath(@PathParam("segment") List<PathSegment> segments) throws NamespaceException, BadRequestException {
    List<String> pathList = new ArrayList<>();

    for (PathSegment segment : segments) {
      pathList.add(segment.getPath());
    }

    Optional<CatalogEntity> entity = catalogServiceHelper.getCatalogEntityByPath(pathList);

    if (!entity.isPresent()) {
      throw new NotFoundException(String.format("Could not find entity with path [%s]", pathList));
    }

    return entity.get();
  }
}
