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
package com.dremio.dac.resource;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;

/**
 * run external sql
  */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("sql")
public class SQLResource {

  private final JobsService jobs;
  private final SecurityContext securityContext;

  @Inject
  public SQLResource(JobsService jobs, SecurityContext securityContext) {
    this.jobs = jobs;
    this.securityContext = securityContext;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JobDataFragment query(CreateFromSQL sql) {
    SqlQuery query = new SqlQuery(sql.getSql(), sql.getContext(), securityContext);
    // Pagination is not supported in this API, so we need to truncate the results to 500 records
    return new JobUI(jobs.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.REST)
        .build(), NoOpJobStatusListener.INSTANCE)).getData().truncate(500);
  }

}

