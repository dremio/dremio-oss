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

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.service.datasets.DatasetDownloadManager.DOWNLOAD_FROM_JOBS_STORE;
import static com.dremio.dac.service.datasets.DatasetDownloadManager.DOWNLOAD_FROM_JOBS_STORE_ALWAYS;
import static com.dremio.dac.service.datasets.DatasetDownloadManager.DOWNLOAD_RECORDS_LIMIT;
import static com.dremio.dac.service.datasets.DatasetDownloadManager.getTableName;
import static com.dremio.dac.service.datasets.DatasetDownloadManager.getTableOptions;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.jobs.JobsService;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class TestDatasetDownloadManager {
  @Mock private JobsService jobsService;
  @Mock private Path storageLocation;
  @Mock private FileSystem fs;
  private OptionManager optionManager;

  private static final String SELECT_SQL = "SELECT * FROM sourceName.tableName";
  private static final String SHOW_TABLE_SQL = "SHOW TABLES IN sourceName";
  private static final String JOB_ID = UUID.randomUUID().toString();
  private static final Path DOWNLOAD_FILE_PATH = Path.of(UUID.randomUUID().toString());

  @BeforeEach
  public void setUp() throws Exception {
    optionManager = mock(OptionManager.class);
    // default settings
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE_ALWAYS)).thenReturn(false);
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE)).thenReturn(true);
    when(optionManager.getOption(DOWNLOAD_RECORDS_LIMIT)).thenReturn(1_000_000L);
  }

  private String getTargetSql(boolean getDataFromJobsResultsDirectly, String originalSql) {
    final long limit = this.optionManager.getOption(DOWNLOAD_RECORDS_LIMIT);
    String targetSql;
    if (getDataFromJobsResultsDirectly) {
      targetSql = format("SELECT * FROM sys.job_results.%s", SqlUtils.quoteIdentifier(JOB_ID));
      if (limit > 0) {
        targetSql += format(" LIMIT %d", limit);
      }
    } else {
      if (limit > 0) {
        targetSql = format("SELECT * FROM (\n%s\n) LIMIT %d", originalSql, limit);
      } else {
        targetSql = format("\n%s\n", originalSql);
      }
    }
    return targetSql;
  }

  private String getExpectedCtasSql(String tableName, String tableOptions, String targetQuery) {
    return format(
        "CREATE TABLE %s STORE AS (%s) WITH SINGLE WRITER AS %s",
        tableName, tableOptions, targetQuery);
  }

  private void testDownloadQueryResultHelper(
      boolean isJobResultsPDFSBased, String originalSql, DownloadFormat downloadFormat) {
    DatasetDownloadManager datasetDownloadManager =
        new DatasetDownloadManager(
            jobsService, storageLocation, fs, isJobResultsPDFSBased, optionManager);
    boolean getDataFromJobsResultsDirectly = datasetDownloadManager.shouldDownloadFromJobsStore();

    String actualSql =
        datasetDownloadManager.generateCtasSql(
            getDataFromJobsResultsDirectly,
            originalSql,
            JOB_ID,
            DOWNLOAD_FILE_PATH,
            downloadFormat);

    String expectedSql =
        getExpectedCtasSql(
            getTableName(DATASET_DOWNLOAD_STORAGE_PLUGIN, DOWNLOAD_FILE_PATH.toString()),
            getTableOptions(downloadFormat),
            getTargetSql(getDataFromJobsResultsDirectly || originalSql != SELECT_SQL, originalSql));

    assertEquals(expectedSql, actualSql);
  }

  @Test
  public void testDownloadSelectSqlWithPdfsBasedJobsStore() {
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithPdfsBasedJobsStore() {
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadSelectSqlWithNonPdfsBasedJobsStore() {
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithNonPdfsBasedJobsStore() {
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadSelectSqlWithoutLimitWithPdfsBasedJobsStore() {
    when(optionManager.getOption(DOWNLOAD_RECORDS_LIMIT)).thenReturn(-1L);

    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithoutLimitWithPdfsBasedJobsStore() {
    when(optionManager.getOption(DOWNLOAD_RECORDS_LIMIT)).thenReturn(-1L);

    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadSelectSqlWithoutLimitWithNonPdfsBasedJobsStore() {
    when(optionManager.getOption(DOWNLOAD_RECORDS_LIMIT)).thenReturn(-1L);

    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithoutLimitWithNonPdfsBasedJobsStore() {
    when(optionManager.getOption(DOWNLOAD_RECORDS_LIMIT)).thenReturn(-1L);

    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadSelectSqlWithPdfsBasedJobsStoreWhenAlwaysDownloadingFromStoreKeyOn() {
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE_ALWAYS)).thenReturn(true);

    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithPdfsBasedJobsStoreWhenAlwaysDownloadingFromStoreKeyOn() {
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE_ALWAYS)).thenReturn(true);

    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(true, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadSelectSqlWithNonPdfsBasedJobsStoreWhenDownloadingFromStoreKeyOff() {
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE)).thenReturn(false);

    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SELECT_SQL, DownloadFormat.PARQUET);
  }

  @Test
  public void testDownloadShowTablesSqlWithNonPdfsBasedJobsStoreWhenDownloadingFromStoreKeyOff() {
    when(optionManager.getOption(DOWNLOAD_FROM_JOBS_STORE)).thenReturn(false);

    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.JSON);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.CSV);
    testDownloadQueryResultHelper(false, SHOW_TABLE_SQL, DownloadFormat.PARQUET);
  }
}
