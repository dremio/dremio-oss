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

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.AnalyzeRequest;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.SuggestionResponse;
import com.dremio.dac.explore.model.ValidationResponse;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.model.job.QueryError;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.planner.sql.SQLAnalyzer;
import com.dremio.exec.planner.sql.SQLAnalyzerFactory;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobsService;
import com.google.common.base.Joiner;

/**
 * run external sql
  */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sql")
public class SQLResource extends BaseResourceWithAllocator {

  private final JobsService jobs;
  private final SecurityContext securityContext;
  private final SabotContext sabotContext;
  private final ProjectOptionManager projectOptionManager;

  @Inject
  public SQLResource(SabotContext sabotContext, JobsService jobs, SecurityContext securityContext, BufferAllocatorFactory allocatorFactory,
                     ProjectOptionManager projectOptionManager) {
    super(allocatorFactory);
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.sabotContext = sabotContext;
    this.projectOptionManager = projectOptionManager;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public JobDataFragment query(CreateFromSQL sql) {
    final SqlQuery query = JobRequestUtil.createSqlQuery(sql.getSql(), sql.getContext(), securityContext.getUserPrincipal().getName());
    // Pagination is not supported in this API, so we need to truncate the results to 500 records
    final CompletionListener listener = new CompletionListener();
    final JobId jobId = jobs.submitJob(SubmitJobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.REST).build(), listener);
    listener.awaitUnchecked();
    return new JobDataWrapper(jobs, jobId, securityContext.getUserPrincipal().getName()).truncate(getOrCreateAllocator("query"), 500);
  }

  @POST
  @Path("/analyze/suggest")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SuggestionResponse suggestSQL(AnalyzeRequest analyzeRequest) {
    final String sql = analyzeRequest.getSql();
    final List<String> context = analyzeRequest.getContext();
    final int cursorPosition = analyzeRequest.getCursorPosition();

    // Setup dependencies and execute suggestion acquisition
    SQLAnalyzer SQLAnalyzer =
      SQLAnalyzerFactory.createSQLAnalyzer(
        securityContext.getUserPrincipal().getName(), sabotContext, context, true, projectOptionManager);

    List<SqlMoniker> sqlEditorHints = SQLAnalyzer.suggest(sql, cursorPosition);

    // Build response object and return
    return buildSuggestionResponse(sqlEditorHints);
  }

  @POST
  @Path("/analyze/validate")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ValidationResponse validateSQL(AnalyzeRequest analyzeRequest) {

    final String sql = analyzeRequest.getSql();
    final List<String> context = analyzeRequest.getContext();

    // Setup dependencies and execute validation
    SQLAnalyzer SQLAnalyzer =
      SQLAnalyzerFactory.createSQLAnalyzer(
        securityContext.getUserPrincipal().getName(), sabotContext, context, false, projectOptionManager);

    List<SqlAdvisor.ValidateErrorInfo> validationErrors = SQLAnalyzer.validate(sql);

    // Build response object and return
    return buildValidationResponse(validationErrors);
  }


  /**
   * Builds the response object for query suggestions.
   *
   * @param suggestionList  The suggestion list returned from the SqlAdvisor.
   *
   * @return The built SuggestionResponse object or null if there are no suggestions.
   */
  public SuggestionResponse buildSuggestionResponse(List<SqlMoniker> suggestionList) {

    // Return empty response in REST request
    if (suggestionList == null || suggestionList.isEmpty()) {
      return null;
    }

    // Create and populate suggestion response list
    List<SuggestionResponse.Suggestion> suggestions = new ArrayList<>();
    for (SqlMoniker hint : suggestionList) {

      // Quote the identifiers if they are not keywords or functions,
      // and are required to be quoted.
      List<String> qualifiedNames = hint.getFullyQualifiedNames();
      if ((hint.getType() != SqlMonikerType.KEYWORD) && (hint.getType() != SqlMonikerType.FUNCTION)) {
        qualifiedNames = qualifiedNames.stream().map(name -> quoteIdentifier(name)).collect(Collectors.toList());
      }

      suggestions.add(
        new SuggestionResponse.Suggestion(Joiner.on(".").join(qualifiedNames),hint.getType().name()));
    }

    SuggestionResponse response = new SuggestionResponse(suggestions);
    return response;
  }

  /**
   * Builds the response object for query validation.
   *
   * @param errorList  The list of query errors returned from the SqlAdvisor.
   *
   * @return The built ValidationResponse object or null if there are no available validation errors.
   */
  protected ValidationResponse buildValidationResponse(List<SqlAdvisor.ValidateErrorInfo> errorList) {

    // Return empty response in REST request
    if (errorList == null || errorList.isEmpty()) {
      return null;
    }

    // Create and populate error response list
    List<QueryError> sqlErrors = new ArrayList<>();
    for (SqlAdvisor.ValidateErrorInfo error : errorList) {
      sqlErrors.add(
        new QueryError(error.getMessage(),
        new QueryError.Range(error.getStartLineNum(),
          error.getStartColumnNum(),
          error.getEndLineNum() + 1,
          error.getEndColumnNum() + 1)));
    }

    ValidationResponse response = new ValidationResponse(sqlErrors);
    return response;
  }
}

