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
package com.dremio.dac.service.datasets;

import static java.lang.String.format;

import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.util.JobRequestUtil;
import com.dremio.exec.planner.sql.parser.ParserUtil;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.service.job.DownloadSettings;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Schedule download jobs and read output of job for dataset download */
@Options
public class DatasetDownloadManager {
  private static final Logger logger = LoggerFactory.getLogger(DatasetDownloadManager.class);

  public static final String DATASET_DOWNLOAD_STORAGE_PLUGIN = "__datasetDownload";

  /**
   * If set to true, download will request data from job results store directly for *NON-pdfs* job
   * storages
   */
  public static final BooleanValidator DOWNLOAD_FROM_JOBS_STORE =
      new BooleanValidator("dac.download.from_jobs_store", true);

  /** If set to true, download will always request data directly from job results store */
  public static final BooleanValidator DOWNLOAD_FROM_JOBS_STORE_ALWAYS =
      new BooleanValidator("dac.download.from_jobs_store.always", false);

  public static final RangeLongValidator DOWNLOAD_RECORDS_LIMIT =
      new RangeLongValidator("dac.download.records_limit", 0L, 1_000_000L, 1_000_000L);

  private static final Map<DownloadFormat, String> extensions =
      ImmutableMap.of(
          DownloadFormat.JSON, "json",
          DownloadFormat.PARQUET, "parquet",
          DownloadFormat.CSV, "csv",
          DownloadFormat.TABLEAU_DATA_EXTRACT, "tde",
          DownloadFormat.TABLEAU_DATA_SOURCE, "tds");

  private final JobsService jobsService;
  private final FileSystem fs;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final Path storageLocation;

  private final boolean isJobResultsPDFSBased;
  private final OptionManager optionManager;

  public DatasetDownloadManager(
      JobsService jobsService,
      Path storageLocation,
      FileSystem fs,
      boolean isJobResultsPDFSBased,
      final OptionManager optionManager) {
    this.jobsService = jobsService;
    this.storageLocation = storageLocation;
    this.fs = fs;
    this.isJobResultsPDFSBased = isJobResultsPDFSBased;
    this.optionManager = optionManager;
  }

  public static String getDownloadFileName(JobId jobId, DownloadFormat downloadFormat) {
    return format("%s.%s", jobId.getId(), extensions.get(downloadFormat));
  }

  public JobId scheduleDownload(
      List<String> datasetPath,
      String sql,
      DownloadFormat downloadFormat,
      List<String> context,
      String userName,
      JobId jobId) {

    final String downloadId = UUID.randomUUID().toString();
    final String fileName = getDownloadFileName(jobId, downloadFormat);
    final Path downloadFilePath = Path.of(downloadId);

    String ctasSql =
        generateCtasSql(
            shouldDownloadFromJobsStore(), sql, jobId.getId(), downloadFilePath, downloadFormat);
    final JobSubmittedListener listener = new JobSubmittedListener();
    jobId =
        jobsService
            .submitJob(
                SubmitJobRequest.newBuilder()
                    .setDownloadSettings(
                        DownloadSettings.newBuilder()
                            .setDownloadId(downloadId)
                            .setFilename(fileName)
                            .setTriggeringJobId(jobId.getId())
                            .setExtension(downloadFormat.name())
                            .build())
                    .setRunInSameThread(shouldDownloadFromJobsStore())
                    .setQueryType(QueryType.UI_EXPORT)
                    .setSqlQuery(JobRequestUtil.createSqlQuery(ctasSql, context, userName))
                    .build(),
                listener)
            .getJobId();
    listener.await();

    logger.debug("Scheduled download job {} for {}", jobId.getId(), datasetPath);
    return jobId;
  }

  boolean shouldDownloadFromJobsStore() {
    if (this.optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE_ALWAYS)) {
      return true;
    }
    // we should read data directly from job result store whenever it is possible. For pdfs we could
    // not guarantee
    // an order in which we scan the results, that is why we need to re-run the original sql
    // instead.
    return this.optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE) && !isJobResultsPDFSBased;
  }

  @VisibleForTesting
  protected String generateCtasSql(
      boolean getDataFromJobsResultsDirectly,
      String originalSql,
      String jobId,
      Path downloadFilePath,
      DownloadFormat downloadFormat) {
    final long limit = this.optionManager.getOption(DOWNLOAD_RECORDS_LIMIT);
    final String tableName =
        getTableName(DATASET_DOWNLOAD_STORAGE_PLUGIN, downloadFilePath.toString());
    final String tableOptions = getTableOptions(downloadFormat);
    String ctasSql;

    if (getDataFromJobsResultsDirectly) {
      ctasSql = generateCtasSqlByDownloadingFromJobResults(tableName, tableOptions, jobId, limit);
    } else {
      ctasSql = generateCtasSqlByRerunningOriginalSql(tableName, tableOptions, originalSql, limit);
      if (!ParserUtil.isValidQuery(ctasSql)) {
        // When the CTAS sql is not valid, i.e. the original sql must be non-SELECT, download from
        // the jobs store anyway
        ctasSql = generateCtasSqlByDownloadingFromJobResults(tableName, tableOptions, jobId, limit);
      }
    }

    return ctasSql;
  }

  private String generateCtasSqlByDownloadingFromJobResults(
      String tableName, String tableOptions, String jobId, long limit) {
    String ctasSql =
        format(
            "CREATE TABLE %s STORE AS (%s) WITH SINGLE WRITER AS SELECT * FROM sys.job_results.%s",
            tableName, tableOptions, SqlUtils.quoteIdentifier(jobId));
    // Limit must be a non-negative integer
    if (limit >= 0) {
      ctasSql += format(" LIMIT %d", limit);
    }
    return ctasSql;
  }

  private String generateCtasSqlByRerunningOriginalSql(
      String tableName, String tableOptions, String originalSql, long limit) {
    final String targetQuery;
    // Limit must be a non-negative integer
    if (limit >= 0) {
      targetQuery = format("SELECT * FROM (\n%s\n) LIMIT %d", originalSql, limit);
    } else {
      targetQuery = format("\n%s\n", originalSql);
    }
    return format(
        "CREATE TABLE %s STORE AS (%s) WITH SINGLE WRITER AS %s",
        tableName, tableOptions, targetQuery);
  }

  public DownloadDataResponse getDownloadData(
      DownloadInfo downloadInfo, List<ArrowFileMetadata> resultMetadataList) throws IOException {
    final Path jobDataDir = storageLocation.resolve(downloadInfo.getDownloadId());
    // NFS filesystems has delay before files written by executor shows up in the coordinator.
    // For NFS, fs.exists() will force a refresh if the file is not found
    // No action is taken if it returns false as the code path already handles FileNotFoundException
    fs.exists(jobDataDir);
    final List<FileAttributes> files;
    try (final DirectoryStream<FileAttributes> stream = fs.list(jobDataDir)) {
      files = Lists.newArrayList(stream);
    }
    Preconditions.checkArgument(
        files.size() == 1,
        format(
            "Found %d files in download dir %s, must have only one file.",
            files.size(), jobDataDir));
    final FileAttributes file = files.get(0);
    return new DownloadDataResponse(
        fs.open(file.getPath()), downloadInfo.getFileName(), file.size());
  }

  public void cleanupDownloadData(String downloadId) throws IOException {
    final Path jobDataDir = storageLocation.resolve(downloadId);
    logger.debug("Cleaning up data at {}", jobDataDir);
    fs.delete(jobDataDir, true);
  }

  public static String getTableName(String sourceName, String fileName) {
    return format(
        "%s.%s", SqlUtils.quoteIdentifier(sourceName), SqlUtils.quoteIdentifier(fileName));
  }

  public static String getTableOptions(DownloadFormat downloadFormat) {
    switch (downloadFormat) {
      case JSON:
        return "type => 'json', prettyPrint => false";
      case PARQUET:
        return "type => 'parquet'";
      case CSV:
        return "type => 'text', fieldDelimiter => ',', lineDelimiter => '\r\n'";
      case TABLEAU_DATA_EXTRACT:
        throw new UnsupportedOperationException("TDE format not supported by dataset download");
      case TABLEAU_DATA_SOURCE:
        throw new UnsupportedOperationException("TDS format not supported by dataset download");
      default:
        throw new IllegalArgumentException(
            "Invalid dataset download file format " + downloadFormat);
    }
  }

  /** Download data response after job is complete. */
  public static final class DownloadDataResponse {
    private final InputStream input;
    private final String fileName;
    private final long size;

    public DownloadDataResponse(InputStream input, String fileName, long size) {
      this.input = input;
      this.fileName = fileName;
      this.size = size;
    }

    public InputStream getInput() {
      return input;
    }

    public String getFileName() {
      return fileName;
    }

    public long getSize() {
      return size;
    }
  }
}
