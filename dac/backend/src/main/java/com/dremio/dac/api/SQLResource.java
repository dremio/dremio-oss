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

import java.util.concurrent.CompletableFuture;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.SecurityContext;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.google.common.util.concurrent.Futures;

/**
 * run external sql
 */
@APIResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sql")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class SQLResource {
  private final JobsService jobs;
  private final SecurityContext securityContext;
  private final SabotContext sabotContext;

  /**
   * Query details
   */
  public static class QueryDetails {
    private String id;

    public QueryDetails() {
    }

    public QueryDetails(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
  }

  @Inject
  public SQLResource(JobsService jobs, SecurityContext securityContext, SabotContext sabotContext) {
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.sabotContext = sabotContext;
  }

  @POST
  public QueryDetails runQuery(CreateFromSQL sql) {
    SqlQuery query = new SqlQuery(sql.getSql(), sql.getContext(), securityContext);

    final ExternalId externalId = ExternalIdHelper.generateExternalId();
    final CompletableFuture<Job> jobFuture = jobs.submitJob(externalId,
      JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.REST)
        .build(), NoOpJobStatusListener.INSTANCE);

    // if async disabled, wait until job has been submitted then return
    if (!sabotContext.getOptionManager().getOption(ExecConstants.REST_API_RUN_QUERY_ASYNC)) {
      Futures.getUnchecked(jobFuture);
    }

    return new QueryDetails(JobsServiceUtil.getExternalIdAsJobId(externalId).getId());
  }
}
