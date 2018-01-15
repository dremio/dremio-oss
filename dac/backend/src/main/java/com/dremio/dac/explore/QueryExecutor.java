/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaConfig.SchemaInfoProvider;
import com.dremio.exec.store.SchemaTreeProvider;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.file.FileFormat;

/**
 * A per RequestScoped class used to execute queries.
 */
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

  private static final int MAX_JOBS_TO_SEARCH = 25;

  private final SecurityContext context;
  private final JobsService jobsService;
  private final SchemaTreeProvider schemaProvider;

  @Inject
  public QueryExecutor(JobsService jobsService, SchemaTreeProvider schemaProvider, SecurityContext context) {
    super();
    this.jobsService = jobsService;
    this.schemaProvider = schemaProvider;
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
   * @return
   */
  public JobUI runQueryWithListener(SqlQuery query, QueryType queryType, DatasetPath datasetPath,
                                  DatasetVersion version, JobStatusListener statusListener) {
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
        final Iterable<Job> jobsForDataset = version == null ?
          jobsService.getJobsForDataset(datasetPath.toNamespaceKey(), null, query.getUsername(), MAX_JOBS_TO_SEARCH) :
          jobsService.getJobsForDataset(datasetPath.toNamespaceKey(), version, query.getUsername(), MAX_JOBS_TO_SEARCH);
        for (Job job : jobsForDataset) {
          if (job.getJobAttempt().getInfo().getQueryType() == queryType
            && query.getSql().equals(job.getJobAttempt().getInfo().getSql())
            && job.getJobAttempt().getState() == JobState.COMPLETED
            && job.hasResults()) {
            try {
              statusListener.jobCompleted();
              return new JobUI(jobsService.getJob(job.getJobId()));
            } catch (RuntimeException | JobNotFoundException e) {
              logger.debug("job {} not found for dataset {}", job.getJobId().getId(), messagePath, e);
              // no result
            }
          }
        }
        // no running job found
        logger.debug("job not found. Running a new one: " + messagePath);
      }

      return new JobUI(jobsService.submitJob(JobRequest.newBuilder()
          .setSqlQuery(query)
          .setQueryType(queryType)
          .setDatasetPath(datasetPath.toNamespaceKey())
          .setDatasetVersion(version)
          .build(), statusListener));
    } catch (UserRemoteException e) {
      throw new DACRuntimeException(format("Failure while running %s query for dataset %s :\n%s", queryType, messagePath, query) + "\n" + e.getMessage(), e);
    }
  }

  public JobUI runQuery(SqlQuery query, QueryType queryType, DatasetPath datasetPath, DatasetVersion version) {
    return runQueryWithListener(query, queryType, datasetPath, version, NoOpJobStatusListener.INSTANCE);
  }

  public List<String> getColumnList(final String username, DatasetPath path) {
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(username)
        .setProvider(new SchemaInfoProvider() {
          private final ViewExpansionContext viewExpansionContext = new ViewExpansionContext(this, schemaProvider, username);

          @Override
          public ViewExpansionContext getViewExpansionContext() {
            return viewExpansionContext;
          }

          @Override
          public OptionValue getOption(String optionKey) {
            throw new UnsupportedOperationException();
          }
        })
        .build();
    final SchemaPlus schema = schemaProvider.getRootSchema(schemaConfig);
    final Table table = path.getTable(schema);
    return table.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldNames();
  }

  // TODO, this should be moved to dataset resource and be paginated.
  public JobDataFragment previewPhysicalDataset(String table, FileFormat formatOptions) {
    SqlQuery query = new SqlQuery(format("select * from table(%s (%s))", table, formatOptions.toTableOptions()), null, context.getUserPrincipal().getName());
    // We still need to truncate the results to 500 as the preview physical datasets doesn't support pagination yet
    return new JobUI(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.UI_INITIAL_PREVIEW)
        .build(), NoOpJobStatusListener.INSTANCE)).getData().truncate(500);
  }
}
