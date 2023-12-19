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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobInfoDetailsUI;
import com.dremio.dac.model.job.JobsListingUI;
import com.dremio.dac.model.job.ResultOrder;
import com.dremio.dac.obfuscate.ObfuscationUtils;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Resource for getting all jobs, jobs for dataset with filtering and sorting.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/jobs-listing/v1.0")
public class JobsListingResource {

  private static final Escaper ESCAPER = UrlEscapers.urlFragmentEscaper();

  private final Provider<JobsService> jobsService;
  private final JobsService jobService;
  private final SecurityContext securityContext;
  private final CatalogServiceHelper catalogServiceHelper;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final NamespaceService namespaceService;

  @Inject
  public JobsListingResource(
    Provider<JobsService> jobsService,
    JobsService jobService,
    CatalogServiceHelper catalogServiceHelper,
    ReflectionServiceHelper reflectionServiceHelper,
    NamespaceService namespaceService,
    @Context SecurityContext securityContext) {
    this.jobsService = jobsService;
    this.jobService = jobService;
    this.securityContext = securityContext;
    this.catalogServiceHelper = catalogServiceHelper;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.namespaceService = namespaceService;
  }

  // Get jobs using filters and set order
  @WithSpan
  @GET
  @Produces(APPLICATION_JSON)
  public JobsListingUI getJobs(
    @QueryParam("filter") String filters,
    @QueryParam("sort") String sortColumn,
    @QueryParam("order") ResultOrder order,
    @QueryParam("offset") @DefaultValue("0") int offset,
    @QueryParam("limit") @DefaultValue("100") int limit,
    @QueryParam("level") @DefaultValue("0") int level) {
    final SearchJobsRequest.Builder requestBuilder = SearchJobsRequest.newBuilder();
    requestBuilder.setOffset(offset);
    requestBuilder.setLimit(limit);
    requestBuilder.setUserName(securityContext.getUserPrincipal().getName());
    if (filters != null) {
      requestBuilder.setFilterString(filters);
    }
    if (sortColumn != null) {
      requestBuilder.setSortColumn(sortColumn);
    }
    if (order != null) {
      requestBuilder.setSortOrder(order.toSortOrder());
    }

    List<JobSummary> jobs = ImmutableList.copyOf(jobsService.get().searchJobs(requestBuilder.build()));
    jobs = ObfuscationUtils.obfuscate(jobs, ObfuscationUtils::obfuscate);
    return new JobsListingUI(
      jobs,
      jobService,
      getNext(offset, limit, filters, sortColumn, order, jobs.size(), requestBuilder.getDetailLevel() == SearchJobsRequest.DetailLevel.ONE ? 1 : 0)
    );
  }

  // Get details of job
  @WithSpan
  @GET
  @Path("{jobId}/jobDetails")
  @Produces(APPLICATION_JSON)
  public JobInfoDetailsUI getJobDetails(@PathParam("jobId") String jobId, @QueryParam("detailLevel") @DefaultValue("0") int detailLevel, @QueryParam("attempt") @DefaultValue("1") int attempt) throws JobResourceNotFoundException, NamespaceException {
    JobDetails jobDetails;
    UserBitShared.QueryProfile profile = null;
    final JobInfoDetailsUI jobInfoDetailsUI;
    // AttemptIndex will be required to get Index of LastAttempt of a Job which has most suitable information of JobDetails and Profile information
    int attemptIndex = attempt - 1;
    try {
      jobInfoDetailsUI = new JobInfoDetailsUI();
      JobDetailsRequest detailRequest = JobDetailsRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId).build())
        .setUserName(securityContext.getUserPrincipal().getName())
        .setProvideResultInfo(true)
        .setProvideProfileInfo(true)
        .setAttemptIndex(attemptIndex)
        .build();
      jobDetails = jobService.getJobDetails(detailRequest);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    jobDetails = ObfuscationUtils.obfuscate(jobDetails);
    profile = ObfuscationUtils.obfuscate(jobDetails.getProfile());

    return jobInfoDetailsUI.of(jobDetails, profile, catalogServiceHelper, reflectionServiceHelper, namespaceService, detailLevel, attemptIndex);
  }

  private String getNext(
    final int offset,
    final int limit,
    String filter,
    String sortColumn,
    ResultOrder order,
    int previousReturn,
    int level
  ) {

    // only return a next if we returned a full list.
    if (previousReturn != limit) {
      return null;
    }

    StringBuilder sb = getNext(offset, limit, level);
    if (filter != null) {
      sb.append("&filter=");
      sb.append(esc(filter));
    }

    if (sortColumn != null) {
      sb.append("&sort=");
      sb.append(esc(sortColumn));
    }

    if (order != null) {
      sb.append("&order=");
      sb.append(order.name());
    }
    return sb.toString();
  }

  private StringBuilder getNext(int offset, int limit, int level) {
    StringBuilder sb = new StringBuilder();
    sb.append("/apiv2/jobs-listing/v1.0/?");
    sb.append("offset=");
    sb.append(offset + limit);
    sb.append("&limit=");
    sb.append(limit);
    sb.append("&level=");
    sb.append(level);
    return sb;
  }

  private static String esc(String str) {
    return ESCAPER.escape(str);
  }


  // Get jobs of a reflection
  @WithSpan
  @GET
  @Path("/{reflectionId}/reflection")
  @Produces(APPLICATION_JSON)
  public JobsListingUI getReflectionJobs(
    @PathParam("reflectionId") String reflectionId,
    @QueryParam("offset") @DefaultValue("0") int offset,
    @QueryParam("limit") @DefaultValue("100") int limit
  ) {

    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .buildSilently();
    }

    final SearchReflectionJobsRequest.Builder requestBuilder = SearchReflectionJobsRequest.newBuilder();
    requestBuilder.setUserName(securityContext.getUserPrincipal().getName())
      .setLimit(limit)
      .setOffset(offset)
      .setReflectionId(reflectionId);

    List<JobSummary> jobs = ImmutableList.copyOf(jobsService.get().searchReflectionJobs(requestBuilder.build()));
    jobs = ObfuscationUtils.obfuscate(jobs, ObfuscationUtils::obfuscate);

    return new JobsListingUI(
      jobs,
      jobService,
      getNextReflectionJobs(offset, limit, reflectionId, jobs.size())
    );
  }



  private String getNextReflectionJobs(
    final int offset,
    final int limit,
    final String reflectionId,
    int previousReturn
  ) {
    // only return a next if we returned a full list.
    if(previousReturn != limit){
      return null;
    }
    StringBuilder sb = getNextReflectionJobs(offset, limit, reflectionId);
    return sb.toString();
  }

  private StringBuilder getNextReflectionJobs(int offset, int limit, String reflectionId){
    StringBuilder sb = new StringBuilder();
    sb.append("/apiv2/jobs-listing/v1.0/reflection/");
    sb.append(reflectionId);
    sb.append("/?");
    sb.append("offset=");
    sb.append(offset+limit);
    sb.append("&limit=");
    sb.append(limit);
    return sb;
  }
}
