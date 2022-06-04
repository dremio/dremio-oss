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

import java.util.Locale;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.userpreferences.PreferenceData;
import com.dremio.service.userpreferences.EntityAlreadyInPreferenceException;
import com.dremio.service.userpreferences.EntityNotFoundInPreferenceException;
import com.dremio.service.userpreferences.EntityThresholdReachedException;
import com.dremio.service.userpreferences.UserPreferenceService;
import com.dremio.service.userpreferences.proto.UserPreferenceProto;

@APIResource
@Secured
@Path("/users/preferences")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class UserPreferenceResource {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(UserPreferenceResource.class);

  private final UserPreferenceService userPreferenceService;

  @Inject
  public UserPreferenceResource(UserPreferenceService userPreferenceService) {
    this.userPreferenceService = userPreferenceService;
  }

  @GET
  @Path("/{preferenceType}")
  public PreferenceData getPreferenceByType(@PathParam("preferenceType") String preferenceType) {

    return PreferenceData.fromPreference(userPreferenceService.getPreferenceByType(
      validatePreferenceType(preferenceType)));
  }

  @PUT
  @Path("/{preferenceType}/{entityId}")
  public PreferenceData addEntityToPreference(@PathParam("preferenceType") String preferenceType,
                                              @PathParam("entityId") UUID entityId) {
    try {
      return PreferenceData.fromPreference(userPreferenceService.addEntityToPreference(
        validatePreferenceType(preferenceType),
        entityId));
    } catch (EntityAlreadyInPreferenceException exception) {
      throw new BadRequestException(exception.getMessage());
    } catch (EntityThresholdReachedException exception) {
      throw new ForbiddenException(exception.getMessage());
    }
  }

  @DELETE
  @Path("/{preferenceType}/{entityId}")
  public PreferenceData removeEntityFromPreference(@PathParam("preferenceType") String preferenceType,
                                                   @PathParam("entityId") UUID entityId) {
    try {
      return PreferenceData.fromPreference(userPreferenceService.removeEntityFromPreference(
        validatePreferenceType(preferenceType),
        entityId));
    } catch (EntityNotFoundInPreferenceException exception) {
      throw new NotFoundException(exception.getMessage());
    }
  }

  private UserPreferenceProto.PreferenceType validatePreferenceType(String preferenceType) {
    try {
      return UserPreferenceProto.PreferenceType.valueOf(preferenceType.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(String.format("%s is not a valid preference type.",
                                                       preferenceType));
    }
  }
}
