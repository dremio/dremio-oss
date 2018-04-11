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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobsUI;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

/**
 * Resource for getting all jobs, jobs for dataset with filtering and sorting.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/jobs")
public class JobsResource {

  private static final Escaper ESCAPER = UrlEscapers.urlFragmentEscaper();

  private final Provider<JobsService> jobsService;
  private final SecurityContext securityContext;
  private final NamespaceService namespace;

  @Inject
  public JobsResource(
      Provider<JobsService> jobsService,
      NamespaceService namespace,
      @Context SecurityContext securityContext) {
    this.jobsService = jobsService;
    this.securityContext = securityContext;
    this.namespace = namespace;
  }

  // Get jobs using filters and set order
  @GET
  @Produces(APPLICATION_JSON)
  public JobsUI getJobs(
      @QueryParam("filter") String filters,
      @QueryParam("sort") String sortColumn,
      @QueryParam("order") SortOrder order,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("limit") @DefaultValue("100") int limit
      ) {

    final List<Job> jobs = ImmutableList.copyOf(jobsService.get().getAllJobs(filters, sortColumn, order, offset, limit,
      securityContext.getUserPrincipal().getName()));
    return new JobsUI(
        namespace,
        jobs,
        getNext(offset, limit, filters, sortColumn, order, jobs.size())
        );
  }

  private String getNext(final int offset, final int limit, String filter, String sortColumn, SortOrder order, int previousReturn){

    // only return a next if we returned a full list.
    if(previousReturn != limit){
      return null;
    }


    StringBuilder sb = getNext(offset, limit);
    if(filter != null){
      sb.append("&filter=");
      sb.append(esc(filter));
    }

    if(sortColumn != null){
      sb.append("&sort=");
      sb.append(esc(sortColumn));
    }

    if(order != null){
      sb.append("&order=");
      sb.append(order.name());
    }
    return sb.toString();
  }

  private StringBuilder getNext(int offset, int limit){
    StringBuilder sb = new StringBuilder();
    sb.append("/jobs/?");
    sb.append("offset=");
    sb.append(offset+limit);
    sb.append("&limit=");
    sb.append(limit);
    return sb;
  }

  private static String esc(String str){
    return ESCAPER.escape(str);
  }
}
