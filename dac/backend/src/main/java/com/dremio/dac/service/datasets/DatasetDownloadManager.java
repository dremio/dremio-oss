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
package com.dremio.dac.service.datasets;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Schedule download jobs and read output of job for dataset download
 */
public class DatasetDownloadManager {
  private static final Logger logger = LoggerFactory.getLogger(DatasetDownloadManager.class);

  public static final String DATASET_DOWNLOAD_STORAGE_PLUGIN = "__datasetDownload";

  private static final Map<DownloadFormat, String> extensions = ImmutableMap.of(
    DownloadFormat.JSON, "json",
    DownloadFormat.PARQUET, "parquet",
    DownloadFormat.CSV, "csv",
    DownloadFormat.TABLEAU_DATA_EXTRACT, "tde",
    DownloadFormat.TABLEAU_DATA_SOURCE, "tds"
  );

  private final JobsService jobsService;
  private final FileSystem fs;
  private final Path storageLocation;

  public DatasetDownloadManager(JobsService jobsService, Path storageLocation, FileSystem fs) {
    this.jobsService = jobsService;
    this.storageLocation = storageLocation;
    this.fs = fs;
  }

  /**
   * Submit CTAS job for dataset download.
   * @param datasetPath Path of dataset to download
   * @param virtualDatasetUI dataset properties
   * @param downloadFormat output format options for download
   * @param limit number of records to include in output (-1 for no limit)
   * @param userName logged in user who is downloading dataset.
   * @return
   * @throws IOException
   */
  public Job scheduleDownload(DatasetPath datasetPath,
                              VirtualDatasetUI virtualDatasetUI,
                              DownloadFormat downloadFormat,
                              int limit,
                              String userName) throws IOException {
    final DatasetUI datasetUI;
    try {
      datasetUI = DatasetUI.newInstance(virtualDatasetUI, null);
    } catch (NamespaceException ex) {
      // This should never happen. TODO: only reason we create the DatasetUI is to get the resolved path of the dataset.
      // Should move the logic of resolving the dataset path to a common method.
      throw new IOException(ex);
    }
    final String downloadId = UUID.randomUUID().toString();
    final String fileName = format("%s.%s", PathUtils.slugify(datasetUI.getDisplayFullPath()), extensions.get(downloadFormat));
    final Path downloadFilePath = new Path(downloadId);

    final String selectQuery;
    if (limit != -1) {
      selectQuery = format("SELECT * FROM (%s) LIMIT %d", virtualDatasetUI.getSql(), limit);
    } else {
      selectQuery = virtualDatasetUI.getSql();
    }

    String ctasSql = format("CREATE TABLE %s.%s STORE AS (%s) WITH SINGLE WRITER AS %s",
      SqlUtils.quoteIdentifier(DATASET_DOWNLOAD_STORAGE_PLUGIN), SqlUtils.quoteIdentifier(downloadFilePath.toString()), getTableOptions(downloadFormat), selectQuery);

    final Job job = jobsService.submitJob(
        JobRequest.newDownloadJobBuilder(downloadId, fileName)
            .setSqlQuery(new SqlQuery(ctasSql, virtualDatasetUI.getContextList(), userName))
            .build(), NoOpJobStatusListener.INSTANCE);
    logger.debug("Scheduled download job {} for {}", job.getJobId(), datasetPath);
    return job;
  }

  public DownloadDataResponse getDownloadData(DownloadInfo downloadInfo) throws IOException {
    final Path jobDataDir = new Path(storageLocation, downloadInfo.getDownloadId());
    final FileStatus[] files = fs.listStatus(jobDataDir);
    Preconditions.checkArgument(files.length == 1, format("Found %d files in download dir %s, must have only one file.", files.length, jobDataDir));
    return new DownloadDataResponse(fs.open(files[0].getPath()), downloadInfo.getFileName(), files[0].getLen());
  }

  public void cleanupDownloadData(String downloadId) throws IOException {
    final Path jobDataDir = new Path(storageLocation, downloadId);
    logger.debug("Cleaning up data at {}", jobDataDir);
    fs.delete(jobDataDir, true);
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
        throw new IllegalArgumentException("Invalid dataset download file format " + downloadFormat);
    }
  }

  /**
   * Download data response after job is complete.
   */
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
