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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.userpreferences.Entity;
import com.dremio.dac.model.userpreferences.PreferenceData;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.userpreferences.EntityAlreadyInPreferenceException;
import com.dremio.service.userpreferences.EntityNotFoundInPreferenceException;
import com.dremio.service.userpreferences.EntityThresholdReachedException;
import com.dremio.service.userpreferences.UserPreferenceService;
import com.dremio.service.userpreferences.proto.UserPreferenceProto;
import com.google.protobuf.util.Timestamps;

@APIResource
@Secured
@Path("/users/preferences")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class UserPreferenceResource {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(UserPreferenceResource.class);

  private final UserPreferenceService userPreferenceService;
  private final NamespaceService namespaceService;

  @Inject
  public UserPreferenceResource(UserPreferenceService userPreferenceService,
                                NamespaceService namespaceService) {
    this.userPreferenceService = userPreferenceService;
    this.namespaceService = namespaceService;
  }

  @GET
  @Path("/{preferenceType}")
  public PreferenceData getPreferenceByType(
    @PathParam("preferenceType") String preferenceType,
    @QueryParam("showCatalogInfo") @DefaultValue("false") boolean showCatalogInfo)
    throws NamespaceNotFoundException {

    UserPreferenceProto.Preference preference =
      userPreferenceService.getPreferenceByType(validatePreferenceType(preferenceType));
    if (showCatalogInfo) {

      Map<String, UserPreferenceProto.Entity> entityIdToEntityMap = preference.getEntitiesList()
        .stream()
        .collect(Collectors.toMap(UserPreferenceProto.Entity::getEntityId, entity -> entity));

      List<NameSpaceContainer> entities =
        namespaceService.getEntitiesByIds(preference.getEntitiesList()
                                            .stream()
                                            .map(UserPreferenceProto.Entity::getEntityId)
                                            .collect(
                                              Collectors.toList()));
      return new PreferenceData(preference.getType(),
                                entities.parallelStream()
                                  .map(container -> getEntityFromNameSpaceContainer(
                                    container,
                                    entityIdToEntityMap))
                                  .collect(
                                    Collectors.toList()));
    }
    return new PreferenceData(preference.getType(),
                              preference.getEntitiesList().stream().map(
                                entity -> new Entity(entity.getEntityId(),
                                                     null,
                                                     null,
                                                     null,
                                                     Timestamps.toMillis(entity.getTimestamp()))
                              ).collect(
                                Collectors.toList()));
  }

  @PUT
  @Path("/{preferenceType}/{entityId}")
  public PreferenceData addEntityToPreference(@PathParam("preferenceType") String preferenceType,
                                              @PathParam("entityId") UUID entityId) {
    try {
      UserPreferenceProto.Preference preference = userPreferenceService.addEntityToPreference(
        validatePreferenceType(preferenceType),
        entityId);
      return new PreferenceData(preference.getType(),
                                preference.getEntitiesList().stream().map(
                                  entity -> new Entity(entity.getEntityId(),
                                                       null,
                                                       null,
                                                       null,
                                                       Timestamps.toMillis(entity.getTimestamp()))
                                ).collect(
                                  Collectors.toList()));
    } catch (EntityAlreadyInPreferenceException exception) {
      throw new BadRequestException(exception.getMessage());
    } catch (EntityThresholdReachedException | IllegalAccessException exception) {
      throw new ForbiddenException(exception.getMessage());
    }
  }

  @DELETE
  @Path("/{preferenceType}/{entityId}")
  public PreferenceData removeEntityFromPreference(@PathParam("preferenceType") String preferenceType,
                                                   @PathParam("entityId") UUID entityId) {
    try {
      UserPreferenceProto.Preference preference = userPreferenceService.removeEntityFromPreference(
        validatePreferenceType(preferenceType),
        entityId);
      return new PreferenceData(preference.getType(),
                                preference.getEntitiesList().stream().map(
                                  entity -> new Entity(entity.getEntityId(),
                                                       null,
                                                       null,
                                                       null,
                                                       Timestamps.toMillis(entity.getTimestamp()))
                                ).collect(
                                  Collectors.toList()));
    } catch (EntityNotFoundInPreferenceException exception) {
      throw new NotFoundException(exception.getMessage());
    }
  }

  public UserPreferenceProto.PreferenceType validatePreferenceType(String preferenceType) {
    try {
      return UserPreferenceProto.PreferenceType.valueOf(preferenceType.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(String.format("%s is not a valid preference type.",
                                                       preferenceType));
    }
  }

  private Entity getEntityFromNameSpaceContainer(
    NameSpaceContainer container,
    Map<String, UserPreferenceProto.Entity> entityIdToEntityMap) {
    String name = "";
    String type = container.getType().toString();
    EntityId entityId = null;

    if (container.getType() == NameSpaceContainer.Type.HOME) {
      name = HomeName.getUserHomePath(container.getHome().getOwner()).toString();
      entityId = container.getHome().getId();
    } else if (container.getType() == NameSpaceContainer.Type.SPACE) {
      name = container.getSpace().getName();
      entityId = container.getSpace().getId();
    } else if (container.getType() == NameSpaceContainer.Type.SOURCE) {
      name = container.getSource().getName();
      entityId = container.getSource().getId();
    } else if (container.getType() == NameSpaceContainer.Type.DATASET) {
      name = container.getDataset().getName();
      type = container.getDataset().getType().toString();
      entityId = container.getDataset().getId();
    } else if (container.getType() == NameSpaceContainer.Type.FOLDER) {
      name = container.getFolder().getName();
      entityId = container.getFolder().getId();
    }

    return new Entity(entityId.getId(),
                      name,
                      container.getFullPathList(),
                      type,
                      Timestamps.toMillis(
                        entityIdToEntityMap.get(entityId.getId())
                          .getTimestamp()));

  }

}
