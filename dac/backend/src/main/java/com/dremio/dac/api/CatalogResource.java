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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.service.namespace.NamespaceException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Catalog API resource. */
@APIResource
@Secured
@RolesAllowed({"user", "admin"})
@Path("/catalog")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class CatalogResource {
  private static final Logger logger = LoggerFactory.getLogger(CatalogResource.class);

  private static final int MAX_ENTITIES_TO_GET_IN_BULK = 50;
  private static final int MAX_CHILDREN_TO_GET_IN_BULK = 20;

  private final CatalogServiceHelper catalogServiceHelper;

  @Inject
  public CatalogResource(CatalogServiceHelper catalogServiceHelper) {
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @GET
  public ResponseList<? extends CatalogItem> listTopLevelCatalog(
      @QueryParam("include") final List<String> include) {
    return new ResponseList<>(catalogServiceHelper.getTopLevelCatalogItems(include));
  }

  @GET
  @Path("/{id}")
  public CatalogEntity getCatalogItem(
      @PathParam("id") String id,
      @QueryParam("include") List<String> include,
      @QueryParam("exclude") List<String> exclude,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("maxChildren") Integer maxChildren)
      throws NamespaceException {
    CatalogPageToken catalogPageToken = null;
    if (!Strings.isNullOrEmpty(pageToken)) {
      catalogPageToken = CatalogPageToken.fromApiToken(pageToken);
    }

    Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityById(
            id, include, exclude, catalogPageToken, maxChildren);

    if (entity.isEmpty()) {
      throw new NotFoundException(String.format("Could not find entity with id [%s]", id));
    }

    return entity.get();
  }

  @POST
  public CatalogEntity createCatalogItem(CatalogEntity entity)
      throws NamespaceException, BadRequestException {
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
  public Dataset promoteToDataset(Dataset dataset, @PathParam("id") String id)
      throws NamespaceException, BadRequestException {
    try {
      return catalogServiceHelper.promoteToDataset(id, dataset);
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @PUT
  @Path("/{id}")
  public CatalogEntity updateCatalogItem(CatalogEntity entity, @PathParam("id") String id)
      throws NamespaceException, BadRequestException {
    try {
      return catalogServiceHelper.updateCatalogItem(entity, id);
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } catch (ExecutionSetupException | IOException e) {
      throw new InternalServerErrorException(e);
    } catch (UnsupportedOperationException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @DELETE
  @Path("/{id}")
  public void deleteCatalogItem(@PathParam("id") String id, @QueryParam("tag") String tag)
      throws NamespaceException, BadRequestException {
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
  public CatalogEntity getCatalogItemByPath(
      @PathParam("segment") List<PathSegment> segments,
      @QueryParam("include") List<String> include,
      @QueryParam("exclude") List<String> exclude,
      @QueryParam("versionType") String versionType,
      @QueryParam("versionValue") String versionValue,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("maxChildren") Integer maxChildren)
      throws NamespaceException, BadRequestException {
    CatalogPageToken catalogPageToken = null;
    if (!Strings.isNullOrEmpty(pageToken)) {
      catalogPageToken = CatalogPageToken.fromApiToken(pageToken);
    }

    List<String> pathList = new ArrayList<>();
    for (PathSegment segment : segments) {
      // with query parameters we may get an empty final segment
      if (!segment.getPath().isEmpty()) {
        pathList.add(segment.getPath());
      }
    }

    final Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityByPath(
            pathList, include, exclude, versionType, versionValue, catalogPageToken, maxChildren);

    if (entity.isEmpty()) {
      throw new NotFoundException(String.format("Could not find entity with path [%s]", pathList));
    }

    return entity.get();
  }

  @GET
  @Path("/search")
  public ResponseList<CatalogItem> search(@QueryParam("query") String query)
      throws NamespaceException {
    return new ResponseList<>(catalogServiceHelper.search(query));
  }

  @POST
  @Path("/by-ids")
  public ResponseList<CatalogEntity> getListByIds(
      List<String> ids, @QueryParam("maxChildren") Integer maxChildren) {
    if (ids.isEmpty()) {
      return new ResponseList<>();
    }

    // Limit number of entities to return.
    if (ids.size() > MAX_ENTITIES_TO_GET_IN_BULK) {
      throw UserException.validationError()
          .message(
              String.format(
                  "Too many ids %d vs maximum %d", ids.size(), MAX_ENTITIES_TO_GET_IN_BULK))
          .buildSilently();
    }

    // Limit number of children.
    if (maxChildren == null || maxChildren == 0 || maxChildren > MAX_CHILDREN_TO_GET_IN_BULK) {
      throw UserException.validationError()
          .message(
              String.format(
                  "maxChildren query parameter must be set to [1,%d]", MAX_CHILDREN_TO_GET_IN_BULK))
          .buildSilently();
    }

    ResponseList<CatalogEntity> responseList = new ResponseList<>();
    ids.forEach(
        id -> {
          try {
            Optional<CatalogEntity> optionalEntity =
                catalogServiceHelper.getCatalogEntityById(
                    id, ImmutableList.of(), ImmutableList.of(), null, maxChildren);
            if (optionalEntity.isPresent()) {
              responseList.add(optionalEntity.get());
            } else {
              responseList.addError(
                  new GenericErrorMessage(String.format("'%s' was not found", id)));
            }
          } catch (Exception e) {
            logger.warn("Exception while bulk getting entities", e);
            responseList.addError(
                new GenericErrorMessage(
                    String.format("'%s' failed with '%s'", id, e.getMessage())));
          }
        });
    return responseList;
  }

  @POST
  @Path("/by-paths")
  public ResponseList<CatalogEntity> getListByPaths(
      List<List<String>> paths,
      @QueryParam("versionType") String versionType,
      @QueryParam("versionValue") String versionValue,
      @QueryParam("maxChildren") Integer maxChildren) {
    if (paths.isEmpty()) {
      return new ResponseList<>();
    }

    // Limit number of entities to return.
    if (paths.size() > MAX_ENTITIES_TO_GET_IN_BULK) {
      throw UserException.validationError()
          .message(
              String.format(
                  "Too many paths %d vs maximum %d", paths.size(), MAX_ENTITIES_TO_GET_IN_BULK))
          .buildSilently();
    }

    // Limit number of children.
    if (maxChildren == null || maxChildren == 0 || maxChildren > MAX_CHILDREN_TO_GET_IN_BULK) {
      throw UserException.validationError()
          .message(
              String.format(
                  "maxChildren query parameter must be set to [1,%d]", MAX_CHILDREN_TO_GET_IN_BULK))
          .buildSilently();
    }

    ResponseList<CatalogEntity> responseList = new ResponseList<>();
    paths.forEach(
        path -> {
          try {
            Optional<CatalogEntity> optionalEntity =
                catalogServiceHelper.getCatalogEntityByPath(
                    path,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    versionType,
                    versionValue,
                    null,
                    maxChildren);
            if (optionalEntity.isPresent()) {
              responseList.add(optionalEntity.get());
            } else {
              responseList.addError(
                  new GenericErrorMessage(String.format("'%s' was not found", path)));
            }
          } catch (Exception e) {
            logger.warn("Exception while bulk getting entities", e);
            responseList.addError(
                new GenericErrorMessage(
                    String.format("'%s' failed with '%s'", path, e.getMessage())));
          }
        });
    return responseList;
  }

  /** MetadataRefreshResponse class */
  public static class MetadataRefreshResponse {
    private final boolean changed;
    private final boolean deleted;

    @JsonCreator
    public MetadataRefreshResponse(
        @JsonProperty("changed") boolean changed, @JsonProperty("deleted") boolean deleted) {
      this.changed = changed;
      this.deleted = deleted;
    }

    public boolean getChanged() {
      return changed;
    }

    public boolean getDeleted() {
      return deleted;
    }
  }
}
