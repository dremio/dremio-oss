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

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.commons.lang3.EnumUtils;

/** Resource for information about reflections. */
@APIResource
@Secured
@Path("/dataset")
@RolesAllowed({"admin", "user"})
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class DatasetResource {
  private ReflectionServiceHelper reflectionServiceHelper;
  private CatalogServiceHelper catalogServiceHelper;

  @Inject
  public DatasetResource(
      ReflectionServiceHelper reflectionServiceHelper, CatalogServiceHelper catalogServiceHelper) {
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @GET
  @Path("/{id}/reflection")
  public ResponseList<Reflection> listReflectionsForDataset(@PathParam("id") String id) {
    Optional<DatasetConfig> dataset = catalogServiceHelper.getDatasetById(id);

    if (!dataset.isPresent()) {
      throw new DatasetNotFoundException(id);
    }

    ResponseList<Reflection> response = new ResponseList<>();
    Iterable<ReflectionGoal> reflectionsForDataset =
        reflectionServiceHelper.getReflectionsForDataset(id);

    for (ReflectionGoal goal : reflectionsForDataset) {
      response.add(reflectionServiceHelper.newReflection(goal));
    }

    return response;
  }

  @POST
  @Path("/{id}/reflection/recommendation")
  public ResponseList<Reflection> getReflectionRecommendationsForDataset(
      @PathParam("id") String id) {
    return getReflectionRecommendationsForDataset(id, Optional.of("ALL"));
  }

  @POST
  @Path("/{id}/reflection/recommendation/{type}")
  public ResponseList<Reflection> getReflectionRecommendationsForDataset(
      @PathParam("id") String id, @PathParam("type") Optional<String> optionalType) {
    Optional<DatasetConfig> dataset = catalogServiceHelper.getDatasetById(id);

    if (!dataset.isPresent()) {
      throw new DatasetNotFoundException(id);
    }

    if (!optionalType.isPresent()) {
      throw UserException.validationError()
          .message(
              String.format("Path parameter of reflection recommendation type is not provided."))
          .build();
    }

    String type = optionalType.get().toUpperCase();
    if (!EnumUtils.isValidEnum(ReflectionSuggestionType.class, type)) {
      throw UserException.validationError()
          .message(
              String.format(
                  "Invalid path parameter of reflection recommendation type: %s",
                  optionalType.get()))
          .build();
    }

    List<Reflection> recommendations =
        Lists.transform(
            reflectionServiceHelper.getRecommendedReflections(
                id, ReflectionSuggestionType.valueOf(type)),
            new Function<ReflectionGoal, Reflection>() {
              @Override
              public Reflection apply(ReflectionGoal goal) {
                return new Reflection(goal);
              }
            });
    return new ResponseList<>(recommendations);
  }
}
