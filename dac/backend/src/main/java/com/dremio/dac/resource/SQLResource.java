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

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.autocomplete.AutocompleteProxy;
import com.dremio.dac.service.autocomplete.model.AutocompleteRequest;
import com.dremio.dac.service.autocomplete.model.AutocompleteResponse;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobsService;
import com.google.common.base.Preconditions;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

/** The REST resource that serves a query API, SQL autocomplete, & a functions list. */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sql")
public class SQLResource extends BaseResourceWithAllocator {
  private final JobsService jobs;
  private final SecurityContext securityContext;
  private final FunctionsListService functionsListService;
  private final CatalogServiceHelper catalogServiceHelper;

  @Inject
  public SQLResource(
      SabotContext sabotContext,
      JobsService jobs,
      SecurityContext securityContext,
      BufferAllocatorFactory allocatorFactory,
      ProjectOptionManager projectOptionManager,
      CatalogServiceHelper catalogServiceHelper) {
    super(allocatorFactory);
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.functionsListService =
        new FunctionsListService(sabotContext, securityContext, projectOptionManager);
    this.catalogServiceHelper = catalogServiceHelper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated
  public JobDataFragment query(CreateFromSQL sql) {
    final SqlQuery sqlQuery =
        JobRequestUtil.createSqlQuery(
            sql.getSql(),
            sql.getContext(),
            securityContext.getUserPrincipal().getName(),
            sql.getEngineName(),
            null,
            sql.getReferences());

    // Pagination is not supported in this API, so we need to truncate the results to 500 records
    final CompletionListener listener = new CompletionListener();

    final JobSubmission jobSubmission =
        jobs.submitJob(
            SubmitJobRequest.newBuilder()
                .setSqlQuery(sqlQuery)
                .setQueryType(QueryType.REST)
                .build(),
            listener);

    listener.awaitUnchecked();

    return new JobDataWrapper(
            jobs,
            jobSubmission.getJobId(),
            jobSubmission.getSessionId(),
            securityContext.getUserPrincipal().getName())
        .truncate(getOrCreateAllocator("query"), 500);
  }

  @POST
  @Path("/autocomplete")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AutocompleteResponse getSuggestions(AutocompleteRequest request) {
    Preconditions.checkNotNull(request);

    return AutocompleteProxy.getSuggestions(catalogServiceHelper, request);
  }

  @GET
  @Path("/functions")
  @Produces(MediaType.APPLICATION_JSON)
  public FunctionsListService.Response getFunctions() {
    return functionsListService.getFunctions();
  }
}
