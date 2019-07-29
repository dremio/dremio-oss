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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.reflection.ReflectionStatusUI;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * Resource for information about reflections.
 */
@APIResource
@Secured
@Path("/dataset")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class DatasetResource {
  private ReflectionServiceHelper reflectionServiceHelper;
  private CatalogServiceHelper catalogServiceHelper;

  @Inject
  public DatasetResource(ReflectionServiceHelper reflectionServiceHelper, CatalogServiceHelper catalogServiceHelper) {
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @GET
  @RolesAllowed({"admin"})
  @Path("/{id}/reflection")
  public ResponseList<Reflection> listReflectionsForDataset(@PathParam("id") String id) {
    Optional<DatasetConfig> dataset = catalogServiceHelper.getDatasetById(id);

    if (!dataset.isPresent()) {
      throw new DatasetNotFoundException(id);
    }

    ResponseList<Reflection> response = new ResponseList<>();
    Iterable<ReflectionGoal> reflectionsForDataset = reflectionServiceHelper.getReflectionsForDataset(id);

    for (ReflectionGoal goal : reflectionsForDataset) {
      String reflectionId = goal.getId().getId();
      ReflectionStatusUI status = reflectionServiceHelper.getStatusForReflection(reflectionId);
      response.add(new Reflection(goal, status, reflectionServiceHelper.getCurrentSize(reflectionId), reflectionServiceHelper.getTotalSize(reflectionId)));
    }

    return response;
  }

  @POST
  @RolesAllowed({"admin"})
  @Path("/{id}/reflection/recommendation")
  public ResponseList<Reflection> getReflectionRecommendationsForDataset(@PathParam("id") String id) {
    Optional<DatasetConfig> dataset = catalogServiceHelper.getDatasetById(id);

    if (!dataset.isPresent()) {
      throw new DatasetNotFoundException(id);
    }

    List<Reflection> recommendations = Lists.transform(reflectionServiceHelper.getRecommendedReflections(id), new Function<ReflectionGoal, Reflection>() {
      @Override
      public Reflection apply(ReflectionGoal goal) {
        return new Reflection(goal);
      }
    });
    return new ResponseList<>(recommendations);
  }
}
