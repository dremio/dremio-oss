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
package com.dremio.service.jobs;

import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Job request.
 */
public final class JobRequest {

  private final RequestType requestType;

  private final SqlQuery sqlQuery;
  private final QueryType queryType;
  private final String username;
  private final List<String> datasetPathComponents;
  private final String datasetVersion;

  private final String downloadId;
  private final String fileName;

  private final MaterializationSummary materializationSummary;
  private final SubstitutionSettings substitutionSettings;

  private JobRequest(RequestType requestType,
                     SqlQuery sqlQuery,
                     QueryType queryType,
                     String username,
                     List<String> datasetPathComponents,
                     String datasetVersion,
                     String downloadId,
                     String fileName,
                     MaterializationSummary materializationSummary,
                     SubstitutionSettings substitutionSettings) {
    this.requestType = requestType;

    this.sqlQuery = sqlQuery;
    this.queryType = queryType;
    this.username = username;
    this.datasetPathComponents = datasetPathComponents;
    this.datasetVersion = datasetVersion;

    this.downloadId = downloadId;
    this.fileName = fileName;

    this.materializationSummary = materializationSummary;
    this.substitutionSettings = substitutionSettings;
  }

  public SqlQuery getSqlQuery() {
    return sqlQuery;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public String getUsername() {
    return username;
  }

  public List<String> getDatasetPathComponents() {
    return datasetPathComponents;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  public SubstitutionSettings getSubstitutionSettings() {
    return substitutionSettings;
  }

  JobInfo asJobInfo(final JobId jobId, final String inSpace) {
    final JobInfo jobInfo = new JobInfo(jobId, sqlQuery.getSql(), datasetVersion, queryType)
        .setSpace(inSpace)
        .setUser(username)
        .setStartTime(System.currentTimeMillis())
        .setDatasetPathList(datasetPathComponents)
        .setResultMetadataList(new ArrayList<ArrowFileMetadata>())
        .setContextList(sqlQuery.getContext());

    if (requestType == RequestType.MATERIALIZATION) {
        jobInfo.setMaterializationFor(materializationSummary);
    }

    if (requestType == RequestType.DOWNLOAD) {
        jobInfo.setDownloadInfo(new DownloadInfo()
          .setDownloadId(downloadId)
          .setFileName(fileName));
    }
    return jobInfo;
  }

  private enum RequestType {
    DEFAULT, DOWNLOAD, MATERIALIZATION
  }

  /**
   * Job request builder.
   */
  public static final class Builder {

    private final RequestType requestType;

    private SqlQuery sqlQuery;
    private QueryType queryType;
    private String username;
    private NamespaceKey datasetPath;
    private DatasetVersion datasetVersion;

    private String downloadId;
    private String fileName;

    private MaterializationSummary materializationSummary;
    private SubstitutionSettings substitutionSettings;

    private Builder(String downloadId, String fileName) {
      this.requestType = RequestType.DOWNLOAD;
      this.downloadId = downloadId;
      this.fileName = fileName;
      this.queryType = QueryType.UI_EXPORT;
    }

    private Builder(MaterializationSummary materializationSummary,
                    SubstitutionSettings substitutionSettings) {
      this.requestType = RequestType.MATERIALIZATION;
      this.materializationSummary = materializationSummary;
      this.substitutionSettings = substitutionSettings;
    }

    private Builder() {
      this.requestType = RequestType.DEFAULT;
    }

    /**
     * Set the sql query to be executed. This is required.
     *
     * @param sqlQuery sql query
     * @return this builder
     */
    public Builder setSqlQuery(SqlQuery sqlQuery) {
      this.sqlQuery = sqlQuery;
      return this;
    }

    /**
     * Set the type of the query. Default is UNKNOWN.
     *
     * @param queryType query type
     * @return this builder
     */
    public Builder setQueryType(QueryType queryType) {
      this.queryType = queryType;
      return this;
    }

    /**
     * Set the username for the job. Provided here, or via {@link #setSqlQuery(SqlQuery)}.
     *
     * @param username username
     * @return this builder
     */
    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Set the substitution settings for this job request.
     * @param substitutionSettings the substitution settings for this run.
     * @return this builder.
     */
    public Builder setSubstitutionSettings(SubstitutionSettings substitutionSettings) {
      this.substitutionSettings = substitutionSettings;
      return this;
    }

    /**
     * Set the dataset path that the job will run against. Optional.
     *
     * @param datasetPath dataset path
     * @return this builder
     */
    public Builder setDatasetPath(NamespaceKey datasetPath) {
      this.datasetPath = datasetPath;
      return this;
    }

    /**
     * Set the version of the dataset that the job will run against. Optional.
     *
     * @param datasetVersion dataset version
     * @return this builder
     */
    public Builder setDatasetVersion(DatasetVersion datasetVersion) {
      this.datasetVersion = datasetVersion;
      return this;
    }

    /**
     * Build the job request.
     *
     * @return job request
     * @throws IllegalArgumentException if arguments are incorrect
     * @throws NullPointerException if arguments are not provided
     */
    public JobRequest build() {
      Preconditions.checkNotNull(sqlQuery, "sql query not provided");

      queryType = MoreObjects.firstNonNull(queryType, QueryType.UNKNOWN);
      username = MoreObjects.firstNonNull(username, sqlQuery.getUsername());

      final List<String> datasetPathComponents =
          datasetPath == null ? ImmutableList.of("UNKNOWN") : datasetPath.getPathComponents();
      final String datasetVersion =
          this.datasetVersion == null ? "UNKNOWN" : this.datasetVersion.getVersion();

      if (requestType == RequestType.DOWNLOAD) {
        Preconditions.checkArgument(queryType == QueryType.UI_EXPORT, "download jobs must be of UI_EXPORT type");
        Preconditions.checkNotNull(downloadId, "download id not provided");
        Preconditions.checkNotNull(fileName, "file name not provided");
      }

      if (requestType == RequestType.MATERIALIZATION) {
        Preconditions.checkNotNull(materializationSummary, "materialization summary not provided");
        Preconditions.checkNotNull(substitutionSettings, "substitution settings not provided");
      }

      return new JobRequest(
          requestType,
          sqlQuery,
          queryType,
          username,
          datasetPathComponents,
          datasetVersion,
          downloadId,
          fileName,
          materializationSummary,
          substitutionSettings);
    }
  }

  /**
   * Create a job builder.
   *
   * @return new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Create a download job builder.
   *
   * @param downloadId download id used to find output of job (required)
   * @param fileName   filename to use when data is downloaded (required)
   * @return new builder
   */
  public static Builder newDownloadJobBuilder(String downloadId,
                                              String fileName) {
    return new Builder(downloadId, fileName);
  }

  /**
   * Create a materialization job builder.
   *
   * @param materializationSummary information related to materialization, like materializationId, layoutId, etc.
   *                               (required)
   * @param substitutionSettings   settings related to substitution (required)
   * @return new builder
   */
  public static Builder newMaterializationJobBuilder(MaterializationSummary materializationSummary,
                                                     SubstitutionSettings substitutionSettings) {
    return new Builder(materializationSummary, substitutionSettings);
  }
}
