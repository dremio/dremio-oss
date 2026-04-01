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

import static com.dremio.dac.server.WebServer.MediaType.TEXT_CSV;
import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.annotations.TemporaryAccess;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobSummaryUI;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.job.ReleasingData;
import com.dremio.dac.model.job.async.AsyncStatus;
import com.dremio.dac.model.job.async.AsyncTaskStatus;
import com.dremio.dac.model.job.async.Problem;
import com.dremio.dac.model.job.async.ProblemTypes;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ConflictException;
import com.dremio.dac.service.errors.InvalidReflectionJobException;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.dac.util.DownloadUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.RecordBatches;
import com.dremio.service.jobs.ReflectionJobValidationException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.security.AccessControlException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource for getting single job summary/overview/details */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job/{jobId}")
public class JobResource extends BaseResourceWithAllocator {
  private static final Logger logger = LoggerFactory.getLogger(JobResource.class);

  private final JobsService jobsService;
  private final DatasetVersionMutator datasetService;
  private final SecurityContext securityContext;
  private final NamespaceService namespace;
  private final ConnectionReader connectionReader;
  private final JobId jobId;
  private final SessionId sessionId;

  private static final Pattern CLICKHOUSE_FROM_PATTERN =
      Pattern.compile(
          "(?i)\\bfrom\\s+(?:\"([^\"]+)\"|([A-Za-z0-9_]+))\\s*\\.\\s*(?:\"([^\"]+)\"|([A-Za-z0-9_]+))\\s*\\.\\s*(?:\"([^\"]+)\"|([A-Za-z0-9_]+))");

  @Inject
  public JobResource(
      JobsService jobsService,
      DatasetVersionMutator datasetService,
      @Context SecurityContext securityContext,
      NamespaceService namespace,
      ConnectionReader connectionReader,
      BufferAllocatorFactory allocatorFactory,
      @PathParam("jobId") JobId jobId,
      @PathParam("sessionId") SessionId sessionId) {
    super(allocatorFactory);
    this.jobsService = jobsService;
    this.datasetService = datasetService;
    this.securityContext = securityContext;
    this.namespace = namespace;
    this.connectionReader = connectionReader;
    this.jobId = jobId;
    this.sessionId = sessionId;
  }

  private void setBasicSpanAttributes() {
    if (sessionId != null) {
      Span.current().setAttribute("sessionId", sessionId.getId());
    }
  }

  /** Get job overview. */
  @WithSpan
  @GET
  @Produces(APPLICATION_JSON)
  public JobUI getJob() throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    return new JobUI(
        jobsService,
        new JobId(jobId.getId()),
        sessionId,
        securityContext.getUserPrincipal().getName());
  }

  @WithSpan
  @POST
  @Path("cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancel() throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    try {
      final String username = securityContext.getUserPrincipal().getName();
      jobsService.cancel(
          CancelJobRequest.newBuilder()
              .setUsername(username)
              .setJobId(JobsProtoUtil.toBuf(jobId))
              .setReason(String.format("Query cancelled by user '%s'", username))
              .build());
      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch (JobNotFoundException e) {
      if (e.getErrorType() == JobNotFoundException.CauseOfFailure.CANCEL_FAILED) {
        throw new ConflictException(
            String.format("Job %s may have completed and cannot be canceled.", jobId.getId()));
      } else {
        throw JobResourceNotFoundException.fromJobNotFoundException(e);
      }
    } catch (JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch (JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  // Get details of job
  @WithSpan
  @GET
  @Path("/details")
  @Produces(APPLICATION_JSON)
  public JobDetailsUI getJobDetail() throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    final JobDetails jobDetails;
    try {
      JobDetailsRequest request =
          JobDetailsRequest.newBuilder()
              .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId.getId()).build())
              .setUserName(securityContext.getUserPrincipal().getName())
              .setProvideResultInfo(true)
              .build();
      jobDetails = jobsService.getJobDetails(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    return JobDetailsUI.of(jobDetails, securityContext.getUserPrincipal().getName());
  }

  // Get summary of job
  @WithSpan
  @GET
  @Path("/summary")
  @Produces(APPLICATION_JSON)
  public JobSummaryUI getJobSummary(
      @QueryParam("maxSqlLength") @DefaultValue("200") int maxSqlLength)
      throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    final JobSummary summary;
    try {
      JobSummaryRequest request =
          JobSummaryRequest.newBuilder()
              .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId.getId()).build())
              .setUserName(securityContext.getUserPrincipal().getName())
              .setMaxSqlLength(maxSqlLength)
              .build();
      summary = jobsService.getJobSummary(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }
    return JobSummaryUI.of(summary, namespace, maxSqlLength);
  }

  public static String getPaginationURL(JobId jobId) {
    return String.format("/job/%s/data", jobId.getId());
  }

  @WithSpan
  @GET
  @Path("/data")
  @Produces(APPLICATION_JSON)
  public JobDataFragment getDataForVersion(
      @QueryParam("limit") int limit, @QueryParam("offset") int offset)
      throws JobResourceNotFoundException {
    setBasicSpanAttributes();

    Preconditions.checkArgument(limit > 0, "Limit should be greater than 0");
    Preconditions.checkArgument(offset >= 0, "Offset should be greater than or equal to 0");

    try {
      final JobSummary jobSummary =
          jobsService.getJobSummary(
              JobSummaryRequest.newBuilder()
                  .setJobId(JobsProtoUtil.toBuf(jobId))
                  .setUserName(securityContext.getUserPrincipal().getName())
                  .build());

      if (!canViewJobResult(jobSummary)) {
        throw new AccessControlException("Not authorized to access the job results");
      }
    } catch (JobNotFoundException e) {
      logger.warn("job not found: {}", jobId);
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    JobDataClientUtils.waitForFinalState(jobsService, jobId);
    Span.current().addEvent("Wait completed");

    // job results in pagination requests.
    final BufferAllocator allocator = getOrCreateAllocator("getDataForVersion");
    JobDataFragment fragment =
        new JobDataWrapper(jobsService, jobId, sessionId, securityContext.getUserPrincipal().getName())
            .range(allocator, offset, limit);
    return maybeFallbackToClickHouseData(fragment, allocator, offset, limit);
  }

  @WithSpan
  @GET
  @Path("/r/{rowNum}/c/{columnName}")
  @Produces(APPLICATION_JSON)
  public Object getCellFullValue(
      @PathParam("rowNum") int rowNum, @PathParam("columnName") String columnName)
      throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    Preconditions.checkArgument(rowNum >= 0, "Row number shouldn't be negative");
    Preconditions.checkNotNull(columnName, "Expected a non-null column name");

    JobDataClientUtils.waitForFinalState(jobsService, jobId);
    Span.current().addEvent("Wait completed");
    try (final JobDataFragment dataFragment =
        maybeFallbackToClickHouseData(
            new JobDataWrapper(
                    jobsService, jobId, sessionId, securityContext.getUserPrincipal().getName())
                .range(getOrCreateAllocator("getCellFullValue"), rowNum, 1),
            getOrCreateAllocator("getCellFullValue"),
            rowNum,
            1)) {

      return dataFragment.extractValue(columnName, 0);
    }
  }

  private JobDataFragment maybeFallbackToClickHouseData(
      JobDataFragment fragment, BufferAllocator allocator, int offset, int limit) {
    if (fragment.getReturnedRowCount() > 0) {
      return fragment;
    }

    try {
      final Optional<ClickHouseQueryContext> context = getClickHouseQueryContext();
      if (context.isEmpty()) {
        return fragment;
      }

      fragment.close();
      return loadClickHouseData(context.get(), allocator, offset, limit);
    } catch (Exception e) {
      logger.warn("Failed to load ClickHouse data directly for job {}", jobId.getId(), e);
      return fragment;
    }
  }

  private Optional<ClickHouseQueryContext> getClickHouseQueryContext()
      throws JobNotFoundException, NamespaceException, ReflectiveOperationException {
    final JobDetailsRequest request =
        JobDetailsRequest.newBuilder()
            .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId.getId()).build())
            .setUserName(securityContext.getUserPrincipal().getName())
            .setProvideResultInfo(true)
            .build();
    final JobDetails details = jobsService.getJobDetails(request);
    final JobInfo info = JobsProtoUtil.getLastAttempt(details).getInfo();

    if (info.getParentsList() == null || info.getParentsList().isEmpty()) {
      return Optional.empty();
    }

    final ParentDatasetInfo parent = info.getParentsList().get(0);
    final List<String> path = parent.getDatasetPathList();
    if (path == null || path.size() < 3) {
      return Optional.empty();
    }

    final String sourceName = path.get(0);
    final SourceConfig sourceConfig = namespace.getSource(new NamespaceKey(sourceName));
    if (!"CLICKHOUSE".equalsIgnoreCase(ConnectionReader.toType(sourceConfig))) {
      return Optional.empty();
    }

    final ConnectionConf<?, ?> connectionConf = connectionReader.getConnectionConf(sourceConfig);
    final String sql = info.getSql();
    final Matcher matcher = CLICKHOUSE_FROM_PATTERN.matcher(sql);
    if (!matcher.find()) {
      return Optional.empty();
    }

    final String sourceInSql = firstNonNull(matcher.group(1), matcher.group(2), sourceName);
    if (!sourceName.equalsIgnoreCase(sourceInSql)) {
      return Optional.empty();
    }

    final String databaseName = firstNonNull(matcher.group(3), matcher.group(4), path.get(1));
    final String tableName = firstNonNull(matcher.group(5), matcher.group(6), path.get(2));

    return Optional.of(
        new ClickHouseQueryContext(
            sourceName,
            readStringField(connectionConf, "hostname", "localhost"),
            readIntField(connectionConf, "port", 8123),
            readStringField(connectionConf, "username", "default"),
            readSecretField(connectionConf, "password"),
            readBooleanField(connectionConf, "useSsl", false),
            databaseName,
            tableName,
            sql));
  }

  private JobDataFragment loadClickHouseData(
      ClickHouseQueryContext context, BufferAllocator allocator, int offset, int limit)
      throws Exception {
    loadClickHouseDriver();
    final String jdbcUrl =
        String.format(
            "jdbc:clickhouse://%s:%d/?compress=0%s",
            context.hostname, context.port, context.useSsl ? "&ssl=true" : "");
    final String baseSql = normalizeClickHouseSql(context);
    final String sql =
        String.format(
            "SELECT * FROM (%s) AS dremio_clickhouse_fallback LIMIT %d OFFSET %d",
            baseSql, limit, offset);

    try (Connection connection =
            DriverManager.getConnection(jdbcUrl, context.username, context.password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      final List<VarCharVector> vectors = new ArrayList<>(columnCount);

      for (int i = 1; i <= columnCount; i++) {
        VarCharVector vector = new VarCharVector(metaData.getColumnLabel(i), allocator);
        vector.allocateNew(limit * 128, Math.max(limit, 1));
        vectors.add(vector);
      }

      int rowCount = 0;
      while (rowCount < limit && resultSet.next()) {
        for (int i = 0; i < columnCount; i++) {
          final String value = resultSet.getString(i + 1);
          if (value == null) {
            vectors.get(i).setNull(rowCount);
          } else {
            vectors.get(i).setSafe(rowCount, value.getBytes(StandardCharsets.UTF_8));
          }
        }
        rowCount++;
      }

      for (VarCharVector vector : vectors) {
        vector.setValueCount(rowCount);
      }

      final VectorContainer container = new VectorContainer();
      container.addCollection(new ArrayList<ValueVector>(vectors));
      container.setRecordCount(rowCount);
      container.buildSchema();

      final RecordBatchData batch = new RecordBatchData(container, allocator);
      final RecordBatchHolder holder = RecordBatchHolder.newRecordBatchHolder(batch, 0, rowCount);
      final RecordBatches recordBatches = new RecordBatches(Collections.singletonList(holder));
      container.close();
      return new JobDataFragmentWrapper(
          offset, ReleasingData.from(recordBatches, jobId, sessionId));
    }
  }

  private static void loadClickHouseDriver() throws ClassNotFoundException {
    try {
      Class.forName("com.dremio.clickhouse.clickhouse.jdbc.ClickHouseDriver");
    } catch (ClassNotFoundException e) {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
    }
  }

  private static String readStringField(ConnectionConf<?, ?> conf, String fieldName, String defaultValue)
      throws ReflectiveOperationException {
    final Object value = conf.getClass().getField(fieldName).get(conf);
    return value == null ? defaultValue : value.toString();
  }

  private static int readIntField(ConnectionConf<?, ?> conf, String fieldName, int defaultValue)
      throws ReflectiveOperationException {
    final Object value = conf.getClass().getField(fieldName).get(conf);
    return value instanceof Number ? ((Number) value).intValue() : defaultValue;
  }

  private static boolean readBooleanField(
      ConnectionConf<?, ?> conf, String fieldName, boolean defaultValue)
      throws ReflectiveOperationException {
    final Object value = conf.getClass().getField(fieldName).get(conf);
    return value instanceof Boolean ? (Boolean) value : defaultValue;
  }

  private static String readSecretField(ConnectionConf<?, ?> conf, String fieldName)
      throws ReflectiveOperationException {
    final Object secretRef = conf.getClass().getField(fieldName).get(conf);
    if (secretRef == null) {
      return "";
    }
    if (secretRef instanceof SecretRef) {
      return Objects.toString(((SecretRef) secretRef).get(), "");
    }
    return Objects.toString(secretRef, "");
  }

  private static String firstNonNull(String first, String second, String fallback) {
    if (!Strings.isNullOrEmpty(first)) {
      return first;
    }
    if (!Strings.isNullOrEmpty(second)) {
      return second;
    }
    return fallback;
  }

  private static final class ClickHouseQueryContext {
    private final String sourceName;
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final boolean useSsl;
    private final String databaseName;
    private final String tableName;
    private final String sql;

    private ClickHouseQueryContext(
        String sourceName,
        String hostname,
        int port,
        String username,
        String password,
        boolean useSsl,
        String databaseName,
        String tableName,
        String sql) {
      this.sourceName = sourceName;
      this.hostname = hostname;
      this.port = port;
      this.username = username;
      this.password = password;
      this.useSsl = useSsl;
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.sql = sql;
    }
  }

  private static String normalizeClickHouseSql(ClickHouseQueryContext context) {
    String sql = stripTrailingSemicolon(context.sql);
    if (Strings.isNullOrEmpty(sql)) {
      return String.format("SELECT * FROM `%s`.`%s`", context.databaseName, context.tableName);
    }

    final String sourcePattern = Pattern.quote(context.sourceName);
    final String dbPattern =
        "(?:\"" + Pattern.quote(context.databaseName) + "\"|" + Pattern.quote(context.databaseName) + ")";
    final String tablePattern =
        "(?:\"" + Pattern.quote(context.tableName) + "\"|" + Pattern.quote(context.tableName) + ")";

    sql =
        sql.replaceAll(
            "(?i)\\b" + sourcePattern + "\\s*\\.\\s*" + dbPattern + "\\s*\\.\\s*" + tablePattern,
            "`" + context.databaseName + "`.`" + context.tableName + "`");
    return sql;
  }

  private static String stripTrailingSemicolon(String sql) {
    if (sql == null) {
      return "";
    }
    String trimmed = sql.trim();
    while (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    return trimmed;
  }

  public static String getDownloadURL(JobDetails jobDetails) {
    if (JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getQueryType() == QueryType.UI_EXPORT) {
      return format("/job/%s/download", jobDetails.getJobId().getId());
    }
    return null;
  }

  /**
   * Export data for job id as a file
   *
   * @param downloadFormat - a format of output file. Also defines a file extension
   * @return
   * @throws IOException
   * @throws JobResourceNotFoundException
   * @throws JobNotFoundException
   */
  @WithSpan
  @GET
  @Path("download")
  @Consumes(MediaType.APPLICATION_JSON)
  @TemporaryAccess
  public Response download(@QueryParam("downloadFormat") DownloadFormat downloadFormat)
      throws JobResourceNotFoundException, JobNotFoundException {
    setBasicSpanAttributes();
    return doDownload(jobId, downloadFormat);
  }

  @WithSpan
  @POST
  @Path("download/submit")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @TemporaryAccess
  public JobId asyncDownloadSubmit(@QueryParam("downloadFormat") DownloadFormat downloadFormat)
      throws JobResourceNotFoundException, JobNotFoundException {
    setBasicSpanAttributes();
    final String currentUser = securityContext.getUserPrincipal().getName();
    final DownloadUtil downloadUtil = new DownloadUtil(jobsService, datasetService);
    return downloadUtil.submitAsyncDownload(jobId, currentUser, downloadFormat);
  }

  @WithSpan
  @GET
  @Path("download/status")
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @TemporaryAccess
  public AsyncTaskStatus asyncDownloadStatus(@QueryParam("downloadJobId") JobId downloadJobId)
      throws JobResourceNotFoundException, JobNotFoundException {
    setBasicSpanAttributes();
    JobDetails downloadJobDetails = getDownloadJobDetails(downloadJobId);
    JobState downloadJobState = JobsProtoUtil.getLastAttempt(downloadJobDetails).getState();
    if (downloadJobState == JobState.COMPLETED) {
      return AsyncTaskStatus.builder().setStatus(AsyncStatus.COMPLETED).build();
    } else if (downloadJobState == JobState.FAILED || downloadJobState == JobState.CANCELED) {
      Problem problem =
          Problem.builder()
              .setType(ProblemTypes.INTERNAL_DOWNLOAD_JOB_NOT_SUCESSFUL)
              .setStatus(Status.INTERNAL_SERVER_ERROR.getStatusCode())
              .build();
      return AsyncTaskStatus.builder()
          .setStatus(AsyncStatus.FAILED)
          .setErrors(Collections.singletonList(problem))
          .build();
    } else {
      return AsyncTaskStatus.builder().setStatus(AsyncStatus.RUNNING).build();
    }
  }

  @WithSpan
  @GET
  @Path("download/download")
  @Consumes(APPLICATION_JSON)
  @Produces({APPLICATION_JSON, TEXT_CSV, APPLICATION_OCTET_STREAM})
  @TemporaryAccess
  public Response asyncDownload(@QueryParam("downloadJobId") JobId downloadJobId)
      throws JobResourceNotFoundException, JobNotFoundException {
    setBasicSpanAttributes();
    JobDetails downloadJobDetails = getDownloadJobDetails(downloadJobId);
    JobState downloadJobState = JobsProtoUtil.getLastAttempt(downloadJobDetails).getState();
    if (downloadJobState == JobState.COMPLETED) {
      final DownloadUtil downloadUtil = new DownloadUtil(jobsService, datasetService);
      final ChunkedOutput<byte[]> output =
          downloadUtil.startChunckedDownload(
              downloadJobDetails, securityContext.getUserPrincipal().getName(), getDelay());

      // Prepare response
      final String contentType;
      DownloadInfo downloadInfo =
          JobsProtoUtil.getLastAttempt(downloadJobDetails).getInfo().getDownloadInfo();
      DownloadFormat downloadFormat = DownloadFormat.valueOf(downloadInfo.getExtension());
      switch (downloadFormat) {
        case JSON:
          contentType = APPLICATION_JSON;
          break;
        case CSV:
          contentType = TEXT_CSV;
          break;
        default:
          contentType = APPLICATION_OCTET_STREAM;
          break;
      }
      final String outputFileName = downloadInfo.getFileName();

      // Send response with the results file
      return Response.ok(output, contentType)
          .header("Content-Disposition", "attachment; filename=\"" + outputFileName + "\"")
          // stops the browser from trying to determine the type of the file based on the content.
          .header("X-Content-Type-Options", "nosniff")
          .build();
    } else {
      Problem problem =
          Problem.builder()
              .setType(ProblemTypes.DOWNLOAD_FILE_NOT_AVAILABLE)
              .setStatus(Status.NOT_FOUND.getStatusCode())
              .build();
      return Response.ok()
          .entity(
              AsyncTaskStatus.builder()
                  .setStatus(AsyncStatus.FAILED)
                  .setErrors(Collections.singletonList(problem))
                  .build())
          .build();
    }
  }

  // Get details of reflection job
  @WithSpan
  @GET
  @Path("/reflection/{reflectionId}/details")
  @Produces(APPLICATION_JSON)
  public JobDetailsUI getReflectionJobDetail(@PathParam("reflectionId") String reflectionId)
      throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    final JobDetails jobDetails;

    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError().message("reflectionId cannot be null or empty").build();
    }

    try {
      JobDetailsRequest.Builder jobDetailsRequestBuilder =
          JobDetailsRequest.newBuilder()
              .setJobId(
                  com.dremio.service.job.proto.JobProtobuf.JobId.newBuilder()
                      .setId(jobId.getId())
                      .build())
              .setProvideResultInfo(true)
              .setUserName(securityContext.getUserPrincipal().getName());

      ReflectionJobDetailsRequest request =
          ReflectionJobDetailsRequest.newBuilder()
              .setJobDetailsRequest(jobDetailsRequestBuilder.build())
              .setReflectionId(reflectionId)
              .build();

      jobDetails = jobsService.getReflectionJobDetails(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }

    return JobDetailsUI.of(jobDetails, securityContext.getUserPrincipal().getName());
  }

  @WithSpan
  @POST
  @Path("/reflection/{reflectionId}/cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancelReflectionJob(@PathParam("reflectionId") String reflectionId)
      throws JobResourceNotFoundException {
    setBasicSpanAttributes();
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError().message("reflectionId cannot be null or empty").build();
    }

    try {
      final String username = securityContext.getUserPrincipal().getName();

      CancelJobRequest cancelJobRequest =
          CancelJobRequest.newBuilder()
              .setUsername(username)
              .setJobId(JobsProtoUtil.toBuf(jobId))
              .setReason(String.format("Query cancelled by user '%s'", username))
              .build();

      CancelReflectionJobRequest cancelReflectionJobRequest =
          CancelReflectionJobRequest.newBuilder()
              .setCancelJobRequest(cancelJobRequest)
              .setReflectionId(reflectionId)
              .build();

      jobsService.cancelReflectionJob(cancelReflectionJobRequest);
      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    } catch (JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch (JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  private JobDetails getDownloadJobDetails(JobId downloadJobId) throws JobNotFoundException {
    final String currentUser = securityContext.getUserPrincipal().getName();
    JobDetails downloadJobDetails =
        jobsService.getJobDetails(
            JobDetailsRequest.newBuilder()
                .setJobId(JobsProtoUtil.toBuf(downloadJobId))
                .setUserName(currentUser)
                .setSkipProfileInfo(true)
                .build());
    DownloadInfo downloadInfo =
        JobsProtoUtil.getLastAttempt(downloadJobDetails).getInfo().getDownloadInfo();
    if (!Objects.equals(downloadInfo.getTriggeringJobId(), jobId.getId())) {
      throw new BadRequestException("downloadJobId is not associated with this jobId");
    }
    return downloadJobDetails;
  }

  protected Response doDownload(JobId previewJobId, DownloadFormat downloadFormat)
      throws JobResourceNotFoundException, JobNotFoundException {
    final String currentUser = securityContext.getUserPrincipal().getName();

    // first check that current user has access to preview data
    final JobDetailsRequest previewJobRequest =
        JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(previewJobId))
            .setUserName(currentUser)
            .build();

    final JobDetails jobDetails = jobsService.getJobDetails(previewJobRequest);

    final DownloadUtil downloadUtil = new DownloadUtil(jobsService, datasetService);
    final ChunkedOutput<byte[]> output =
        downloadUtil.startChunckedDownload(previewJobId, currentUser, downloadFormat, getDelay());

    final String contentType;
    if (downloadFormat != null) {
      switch (downloadFormat) {
        case JSON:
          contentType = APPLICATION_JSON;
          break;
        case CSV:
          contentType = "text/csv";
          break;
        default:
          contentType = MediaType.APPLICATION_OCTET_STREAM;
          break;
      }
    } else {
      contentType = MediaType.APPLICATION_OCTET_STREAM;
    }

    final JobInfo info = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();

    final String outputFileName;
    // job id is already a download job. So just extract a filename from it
    if (info.getQueryType() == QueryType.UI_EXPORT) {
      outputFileName = info.getDownloadInfo().getFileName();
    } else {
      // must use DatasetDownloadManager.getDownloadFileName. If a naming convention is changed, the
      // change should go to
      // DatasetDownloadManager.getDownloadFileName method.
      outputFileName = DatasetDownloadManager.getDownloadFileName(previewJobId, downloadFormat);
    }

    return Response.ok(output, contentType)
        .header("Content-Disposition", "attachment; filename=\"" + outputFileName + "\"")
        // stops the browser from trying to determine the type of the file based on the content.
        .header("X-Content-Type-Options", "nosniff")
        .build();
  }

  protected long getDelay() {
    return 0L;
  }

  protected boolean canViewJobResult(JobSummary jobSummary) {
    return true;
  }
}
