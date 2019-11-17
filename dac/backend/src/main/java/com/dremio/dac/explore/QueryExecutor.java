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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.model.job.JobUI;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.GetJobRequest;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SearchJobsRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.file.FileFormat;
import com.google.common.util.concurrent.Futures;

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
    super();
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
  JobData runQueryWithListener(SqlQuery query, QueryType queryType, DatasetPath datasetPath,
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
      if (queryType != QueryType.UI_RUN) {
        final SearchJobsRequest.Builder requestBuilder = SearchJobsRequest.newBuilder()
            .setDatasetPath(datasetPath.toNamespaceKey())
            .setLimit(MAX_JOBS_TO_SEARCH)
            .setUsername(query.getUsername());
        if (version != null) {
          requestBuilder.setDatasetVersion(version);
        }
        final Iterable<Job> jobsForDataset = jobsService.searchJobs(requestBuilder.build());
        for (Job job : jobsForDataset) {
          if (job.getJobAttempt().getInfo().getQueryType() == queryType
            && query.getSql().equals(job.getJobAttempt().getInfo().getSql())
            && job.getJobAttempt().getState() == JobState.COMPLETED
            && job.hasResults()) {
            try {
              statusListener.jobCompleted();
              GetJobRequest request = GetJobRequest.newBuilder()
                .setJobId(job.getJobId())
                .build();
              return new JobDataWrapper(jobsService.getJob(request).getData());
            } catch (RuntimeException | JobNotFoundException e) {
              logger.debug("job {} not found for dataset {}", job.getJobId().getId(), messagePath, e);
              // no result
            }
          }
        }
        // no running job found
        logger.debug("job not found. Running a new one: " + messagePath);
      }

      final Job job = Futures.getUnchecked(
        jobsService.submitJob(JobRequest.newBuilder()
          .setSqlQuery(query)
          .setQueryType(queryType)
          .setDatasetPath(datasetPath.toNamespaceKey())
          .setDatasetVersion(version)
          .runInSameThread(runInSameThread)
          .build(), statusListener)
      );
      return new JobDataWrapper(job.getData());
    } catch (UserRemoteException e) {
      throw new DACRuntimeException(format("Failure while running %s query for dataset %s :\n%s", queryType, messagePath, query) + "\n" + e.getMessage(), e);
    }
  }

  public JobData runQuery(SqlQuery query, QueryType queryType, DatasetPath datasetPath, DatasetVersion version) {
    return runQueryWithListener(query, queryType, datasetPath, version, NoOpJobStatusListener.INSTANCE);
  }

  public List<String> getColumnList(final String username, DatasetPath path) {
    Catalog catalog = catalogService.getCatalog(SchemaConfig.newBuilder(context.getUserPrincipal().getName()).build());
    DremioTable table = catalog.getTable(path.toNamespaceKey());
    return table.getRowType(SqlTypeFactoryImpl.INSTANCE).getFieldNames();
  }

  @Deprecated
  public JobDataFragment previewPhysicalDataset(String table, FileFormat formatOptions) {
    SqlQuery query = new SqlQuery(format("select * from table(%s (%s))", table, formatOptions.toTableOptions()), null, context.getUserPrincipal().getName());
    // We still need to truncate the results to 500 as the preview physical datasets doesn't support pagination yet
    return JobUI.getJobData(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.UI_INITIAL_PREVIEW)
        .build(), NoOpJobStatusListener.INSTANCE)
    ).truncate(500);
  }
}
