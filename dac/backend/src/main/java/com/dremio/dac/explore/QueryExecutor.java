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
package com.dremio.dac.explore;

import static com.dremio.dac.model.common.RootEntity.RootType.SOURCE;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.DremioEdition;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.MultiJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.users.SystemUser;

/**
 * A per RequestScoped class used to execute queries.
 */
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

  private static final int MAX_JOBS_TO_SEARCH = 25;

  private final JobsService jobsService;
  private final CatalogService catalogService;
  private final SecurityContext context;

  @Inject
  public QueryExecutor(JobsService jobsService, CatalogService catalogService, SecurityContext context) {
    this.jobsService = jobsService;
    this.catalogService = catalogService;
    this.context = context;
  }

  /**
   * Run the query with given listener
   * <p>
   * Virtual Datasets must provide a version
   * Sources' physical datasets have null version
   *
   * @param query          the sql to run
   * @param queryType      the type of query(metadata)
   * @param datasetPath    the path for the dataset represented by the query (metadata)
   * @param version        the version for the dataset represented by the query (metadata)
   * @param statusListener Job status and event listener
   */
  public JobData runQueryWithListener(SqlQuery query, QueryType queryType, DatasetPath datasetPath,
                             DatasetVersion version, JobStatusListener statusListener) {
    return runQueryWithListener(query, queryType, datasetPath, version, statusListener, false);
  }

  /**
   * Run the query with given listener
   * <p>
   * Virtual Datasets must provide a version
   * Sources' physical datasets have null version
   *
   * @param query          the sql to run
   * @param queryType      the type of query(metadata)
   * @param datasetPath    the path for the dataset represented by the query (metadata)
   * @param version        the version for the dataset represented by the query (metadata)
   * @param statusListener Job status and event listener
   * @param runInSameThread runs attemptManager in a single thread
   */
  JobData runQueryWithListener(SqlQuery query, QueryType queryType, DatasetPath datasetPath,
      DatasetVersion version, JobStatusListener statusListener, boolean runInSameThread) {
    String messagePath = datasetPath + (version == null ? "" : "/" + version);
    if (datasetPath.getRoot().getRootType() == SOURCE) {
      if (version != null) {
        throw new IllegalArgumentException("version should be null for physical datasets: " + datasetPath);
      }
    } else {
      checkNotNull(version, "version should not be null for virtual datasets: " + datasetPath);
    }

    try {
      // don't check the cache for UI_RUN queries
      if (queryType != QueryType.UI_RUN && DremioEdition.get() != DremioEdition.MARKETPLACE) {
        final SearchJobsRequest.Builder requestBuilder = SearchJobsRequest.newBuilder()
            .setLimit(MAX_JOBS_TO_SEARCH)
            .setUserName(query.getUsername());
        final VersionedDatasetPath.Builder versionedDatasetPathBuilder = VersionedDatasetPath.newBuilder()
        .addAllPath(datasetPath.toPathList());
        if (version != null) {
          versionedDatasetPathBuilder.setVersion(version.getVersion());
        }
        requestBuilder.setDataset(versionedDatasetPathBuilder.build());
        final Iterable<JobSummary> jobsForDataset = jobsService.searchJobs(requestBuilder.build());
        for (JobSummary job : jobsForDataset) {
          if (job.getQueryType() == JobsProtoUtil.toBuf(queryType)
            && query.getSql().equals(job.getSql())
            && job.getJobState() == JobState.COMPLETED) {
            try {
              if (!jobsService.getJobDetails(
                  JobDetailsRequest.newBuilder()
                      .setJobId(job.getJobId())
                      .setUserName(query.getUsername())
                      .setProvideResultInfo(true)
                      .build())
                  .getHasResults()) {
                continue;
              }

              statusListener.jobCompleted();
              return new JobDataWrapper(jobsService, JobsProtoUtil.toStuff(job.getJobId()), query.getUsername());
            } catch (JobNotFoundException | RuntimeException e) {
              logger.debug("job {} not found for dataset {}", job.getJobId().getId(), messagePath, e);
              // no result
            }
          }
        }
        // no running job found
        logger.debug("job not found. Running a new one: " + messagePath);
      }

      final JobSubmittedListener submittedListener = new JobSubmittedListener();
      final JobId jobId = jobsService.submitJob(
        SubmitJobRequest.newBuilder()
          .setSqlQuery(JobsProtoUtil.toBuf(query))
          .setQueryType(JobsProtoUtil.toBuf(queryType))
          .setVersionedDataset(VersionedDatasetPath.newBuilder()
            .addAllPath(datasetPath.toNamespaceKey().getPathComponents())
            .setVersion(version.getVersion())
            .build())
          .setRunInSameThread(runInSameThread)
          .build(),
        new MultiJobStatusListener(statusListener, submittedListener));
      submittedListener.await();

      return new JobDataWrapper(jobsService, jobId, query.getUsername());
    } catch (UserRemoteException e) {
      throw new DACRuntimeException(format("Failure while running %s query for dataset %s :\n%s", queryType, messagePath, query) + "\n" + e.getMessage(), e);
    }
  }

  public JobData runQueryAndWaitForCompletion(SqlQuery query, QueryType queryType, DatasetPath datasetPath, DatasetVersion version) {
    final CompletionListener listener = new CompletionListener();
    final JobData data = runQueryWithListener(query, queryType, datasetPath, version, listener);
    listener.awaitUnchecked();
    return data;
  }

  public List<String> getColumnList(final String username, DatasetPath path) {
    EntityExplorer entityExplorer = catalogService.getCatalog(MetadataRequestOptions.of(
        SchemaConfig.newBuilder(context.getUserPrincipal().getName())
            .build()));
    DremioTable table = entityExplorer.getTable(path.toNamespaceKey());
    return table.getRowType(SqlTypeFactoryImpl.INSTANCE).getFieldNames();
  }

  @Deprecated
  public JobDataFragment previewPhysicalDataset(String table, FileFormat formatOptions, BufferAllocator allocator) {
    final com.dremio.service.job.SqlQuery query = JobRequestUtil.createSqlQuery(format("select * from table(%s (%s))", table, formatOptions.toTableOptions()),
      null, context.getUserPrincipal().getName());
    // We still need to truncate the results to 500 as the preview physical datasets doesn't support pagination yet
    final CompletionListener listener = new CompletionListener();
    final JobId jobId = jobsService.submitJob(
      SubmitJobRequest.newBuilder().setSqlQuery(query).setQueryType(com.dremio.service.job.QueryType.UI_INITIAL_PREVIEW).build(),
      listener);
    listener.awaitUnchecked();

    return new JobDataWrapper(jobsService, jobId, SystemUser.SYSTEM_USERNAME).truncate(allocator, 500);
  }
}
