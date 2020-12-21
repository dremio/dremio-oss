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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsService;

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
  private final ProjectOptionManager projectOptionManager;

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
  public SQLResource(JobsService jobs, SecurityContext securityContext, ProjectOptionManager projectOptionManager) {
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.projectOptionManager = projectOptionManager;
  }

  @POST
  public QueryDetails runQuery(CreateFromSQL sql) {
    final SqlQuery sqlQuery = JobRequestUtil.createSqlQuery(sql.getSql(), sql.getContext(),securityContext.getUserPrincipal().getName(), sql.getEngineName());
    final JobSubmittedListener listener = new JobSubmittedListener();
    final JobId jobId = jobs.submitJob(SubmitJobRequest.newBuilder()
      .setSqlQuery(sqlQuery)
      .setQueryType(QueryType.REST)
      .build(), listener);

    // if async disabled, wait until job has been submitted then return
    if (!projectOptionManager.getOption(ExecConstants.REST_API_RUN_QUERY_ASYNC)) {
      listener.await();
    }

    return new QueryDetails(jobId.getId());
  }
}
