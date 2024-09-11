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
package com.dremio.dac.service;

import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static org.apache.hadoop.util.Time.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import ch.qos.logback.classic.Level;
import com.dremio.common.util.FileUtils;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.FileFormatUI;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.FormatTools;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.io.file.Path;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;
import com.dremio.test.UserExceptionAssert;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to create, update and execute queries on physical datasets.. If you add a new folder(s) to
 * oss/dac/backend/src/test/resources/datasets, you will likely need to update tests in
 * oss/dac/backend/src/test/java/com/dremio/dac/api/TestPromotion.java.
 */
public class TestPhysicalDatasets extends BaseTestServer {
  private static final ch.qos.logback.classic.Logger rootLogger =
      ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("com.dremio"));
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestPhysicalDatasets.class);
  private static Level originalLogLevel;
  private BufferAllocator allocator;

  @BeforeClass
  public static void initLogLevel() {
    originalLogLevel = rootLogger.getLevel();
    rootLogger.setLevel(Level.DEBUG);
  }

  @AfterClass
  public static void restoreLogLevel() {
    rootLogger.setLevel(originalLogLevel);
  }

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    final SourceService sourceService = getSourceService();
    {
      final NASConf nas = new NASConf();
      nas.path = "/";
      SourceUI source = new SourceUI();
      source.setName("dacfs_test");
      source.setConfig(nas);
      source.setMetadataPolicy(
          UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
      sourceService.registerSourceWithRuntime(source);
    }
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @After
  public void cleanUp() {
    allocator.close();
  }

  private static String getSchemaPath(String file) throws IOException {
    return "dacfs_test."
        + PathUtils.constructFullPath(
            PathUtils.toPathComponents(
                Path.of(FileUtils.getResourceAsFile(file).getAbsolutePath())));
  }

  private static SqlQuery createQuery(String file) throws IOException {
    return new SqlQuery(format("select * from %s", getSchemaPath(file)), DEFAULT_USERNAME);
  }

  private static SqlQuery createQueryWithColumn(String column, String file) throws IOException {
    return new SqlQuery(
        format("select %1$s from %2$s", column, getSchemaPath(file)), DEFAULT_USERNAME);
  }

  private static String getUrlPath(String file) throws IOException {
    return Path.of(FileUtils.getResourceAsFile(file).getAbsolutePath()).toString();
  }

  private void checkCounts(
      String parentPath,
      String name,
      boolean isQueryable,
      int jobCount,
      int descendants,
      int datasetCount)
      throws Exception {
    Folder parent =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/folder/" + parentPath)).buildGet(),
            Folder.class);
    for (com.dremio.file.File file : parent.getContents().getFiles()) {
      if (name.equals(file.getName())) {
        assertEquals(
            "jobCount for file " + parentPath + "/" + name, jobCount, (int) file.getJobCount());
        assertEquals(
            "isQueryable for file " + parentPath + "/" + name, isQueryable, file.isQueryable());
        return;
      }
    }
    for (PhysicalDataset physicalDataset : parent.getContents().getPhysicalDatasets()) {
      if (name.equals(physicalDataset.getDatasetName().getName())) {
        assertEquals(
            "jobCount for physical dataset " + parentPath + "/" + name,
            jobCount,
            (int) physicalDataset.getJobCount());
        return;
      }
    }
    for (Folder folder : parent.getContents().getFolders()) {
      if (name.equals(folder.getName())) {
        assertEquals(
            "isQueryable for folder " + parentPath + "/" + name, isQueryable, folder.isQueryable());
        return;
      }
    }
  }

  private JobRequest sqlQueryRequestFromFile(String file) throws IOException {
    return JobRequest.newBuilder()
        .setSqlQuery(createQuery(file))
        .setQueryType(QueryType.UI_RUN)
        .build();
  }

  private JobRequest sqlQueryRequestFromColumnAndFile(String column, String file)
      throws IOException {
    return JobRequest.newBuilder()
        .setSqlQuery(createQueryWithColumn(column, file))
        .setQueryType(QueryType.UI_RUN)
        .build();
  }

  @Test
  public void testJsonFile() throws Exception {
    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/datasets/users.json"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData.getReturnedRowCount());
      assertEquals(2, jobData.getColumns().size());

      String fileUrlPath = getUrlPath("/datasets/users.json");
      String fileParentUrlPath = getUrlPath("/datasets/");

      JsonFileConfig jsonFileConfig = new JsonFileConfig();
      doc("preview json source file");
      JobDataFragment data =
          expectSuccess(
              getBuilder(getAPIv2().path("/source/dacfs_test/file_preview" + fileUrlPath))
                  .buildPost(Entity.json(jsonFileConfig)),
              JobDataFragment.class);
      assertEquals(3, data.getReturnedRowCount());
      assertEquals(2, data.getColumns().size());

      checkCounts(fileParentUrlPath, "users.json", true, 1, 0, 0);
    }
  }

  @Test
  public void testCommaSeparatedTextFile() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("comma.txt");

    String fileUrlPath = getUrlPath("/datasets/text/comma.txt");
    String fileParentUrlPath = getUrlPath("/datasets/text/");

    doc("preview data for source file");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(4, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());
    checkCounts(
        fileParentUrlPath,
        "comma.txt",
        false /* false because we have not saved dataset yet */,
        0,
        0,
        0); // previews are internal queries
  }

  @Test
  public void testCommaSeparatedCsv() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("comma.csv");
    String fileUrlPath = getUrlPath("/datasets/text/comma.txt");
    String fileParentUrlPath = getUrlPath("/datasets/text/");

    doc("preview data for source file");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(4, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());

    fileConfig.setExtractHeader(true);
    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + fileUrlPath))
            .buildPut(Entity.json(fileConfig)));
    data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(3, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());

    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/datasets/text/comma.txt"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData.getReturnedRowCount());
      assertEquals(3, jobData.getColumns().size());
    }

    checkCounts(fileParentUrlPath, "comma.txt", true, 1, 0, 0);
  }

  @Test
  public void testCommaSeparatedCsvWindowsLineEndings() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setName("comma_windows_lineseparator.csv");
    String fileUrlPath = getUrlPath("/datasets/csv/comma_windows_lineseparator.csv");
    String fileParentUrlPath = getUrlPath("/datasets/csv/");

    doc("preview data for source file");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(4, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());

    fileConfig.setExtractHeader(true);
    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + fileUrlPath))
            .buildPut(Entity.json(fileConfig)));
    data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(3, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());

    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/datasets/csv/comma_windows_lineseparator.csv"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData.getReturnedRowCount());
      assertEquals(3, jobData.getColumns().size());
    }

    checkCounts(fileParentUrlPath, "comma_windows_lineseparator.csv", true, 1, 0, 0);
  }

  @Test
  public void testCommaSeparatedCsvTrimHeader() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    // we set the wrong delimiter to test the header trimming
    fileConfig.setLineDelimiter("\n");
    fileConfig.setTrimHeader(true);
    fileConfig.setExtractHeader(true);
    fileConfig.setName("comma_windows_lineseparator.csv");
    String fileUrlPath = getUrlPath("/datasets/csv/comma_windows_lineseparator.csv");

    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(3, data.getReturnedRowCount());
    assertEquals(3, data.getColumns().size());
    // the column name would be address\r if trimHeader was false
    assertEquals("address", data.getColumns().get(2).getName());

    fileConfig.setTrimHeader(false);
    JobDataFragment data2 =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(3, data2.getReturnedRowCount());
    assertEquals(3, data2.getColumns().size());
    // with header trimming turned off, we should see the \r character
    assertEquals("address\r", data2.getColumns().get(2).getName());
  }

  @Test
  public void commaSeparatedLargeTextFile() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("widetable.txt");

    String fileUrlPath = getUrlPath("/datasets/widetable.txt");
    String fileParentUrlPath = getUrlPath("/datasets/");

    doc("preview data for source file");
    try (AutoCloseable ignore =
        withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX.getOptionName(), "800")) {
      UserExceptionMapper.ErrorMessageWithContext error =
          expectError(
              FamilyExpectation.CLIENT_ERROR,
              getBuilder(getAPIv2().path("/source/dacfs_test/file_preview" + fileUrlPath))
                  .buildPost(Entity.json(fileConfig)),
              UserExceptionMapper.ErrorMessageWithContext.class);
      assertTrue(
          error
              .getErrorMessage()
              .contains("Number of fields in dataset exceeded the maximum number of fields"));
      checkCounts(
          fileParentUrlPath,
          "widetable.txt",
          false /* false because we have not saved dataset yet */,
          0,
          0,
          0);
    }
  }

  @Test
  public void testSchemaChangeExit() throws Exception {
    String fileUrlPath = getUrlPath("/datasets/schemachange");
    JsonFileConfig jsonFileConfig = new JsonFileConfig();
    doc("preview json source file");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/folder_preview" + fileUrlPath))
                .buildPost(Entity.json(jsonFileConfig)),
            JobDataFragment.class);
    assertEquals(1, data.getReturnedRowCount());
    assertEquals(1, data.getColumns().size());
  }

  @Test
  public void testLargeJsonFileWorksWithIncreasedScanLimit() throws Exception {
    try (AutoCloseable ignore =
        withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_SCANNED_MAX.getOptionName(), "1000")) {
      try (JobDataFragment result =
          submitJobAndGetData(
              l(JobsService.class),
              sqlQueryRequestFromFile("/datasets/wide_table.json"),
              0,
              500,
              allocator)) {
        assertEquals(1, result.getReturnedRowCount());
      }
    }
  }

  @Test
  public void testLargeJsonFileScanLimit() {
    UserExceptionAssert.assertThatThrownBy(
        () ->
            submitJobAndGetData(
                l(JobsService.class),
                sqlQueryRequestFromFile("/datasets/wide_table.json"),
                0,
                500,
                allocator));
  }

  @Test
  public void testLargeJsonFileColumnLimit() throws Exception {

    try (AutoCloseable ignore1 =
            withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX.getOptionName(), "800");
        AutoCloseable ignore2 =
            withSystemOption(
                CatalogOptions.METADATA_LEAF_COLUMN_SCANNED_MAX.getOptionName(), "1000")) {
      UserExceptionAssert.assertThatThrownBy(
          () ->
              submitJobAndGetData(
                  l(JobsService.class),
                  sqlQueryRequestFromFile("/datasets/wide_table.json"),
                  0,
                  500,
                  allocator));
    }
  }

  @Test
  public void testLargeNestedJsonFileWorksWithIncreasedScanLimit() throws Exception {
    try (AutoCloseable ignore =
        withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_SCANNED_MAX.getOptionName(), "1000")) {
      try (JobDataFragment result =
          submitJobAndGetData(
              l(JobsService.class),
              sqlQueryRequestFromFile("/datasets/wide_nested_table.json"),
              0,
              500,
              allocator)) {
        assertEquals(1, result.getReturnedRowCount());
      }
    }
  }

  @Test
  public void testLargeNestedJsonFileScanLimit() {
    UserExceptionAssert.assertThatThrownBy(
        () ->
            submitJobAndGetData(
                l(JobsService.class),
                sqlQueryRequestFromFile("/datasets/wide_nested_table.json"),
                0,
                500,
                allocator));
  }

  @Test
  public void testLargeNestedJsonFileColumnLimit() throws Exception {
    try (AutoCloseable ignore1 =
            withSystemOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX.getOptionName(), "800");
        AutoCloseable ignore2 =
            withSystemOption(
                CatalogOptions.METADATA_LEAF_COLUMN_SCANNED_MAX.getOptionName(), "1000")) {
      UserExceptionAssert.assertThatThrownBy(
          () ->
              submitJobAndGetData(
                  l(JobsService.class),
                  sqlQueryRequestFromFile("/datasets/wide_nested_table.json"),
                  0,
                  500,
                  allocator));
    }
  }

  @Test
  public void testParquetFile() throws Exception {
    /*
     * Change the batch_size to 100 from 20 to make sure that all the records in the parquet file are read consistently to avoid flaky issue
     * that only 20 records are read while reading parquet file is slow in preview because there is a time limit for reading in preview.
     * The time limit is 500ms in getData for preview in FormatTools.
     */
    setSystemOption("dac.format.preview.batch_size", "100");
    try {
      String fileUrlPath = getUrlPath("/singlefile_parquet_dir/0_0_0.parquet");
      String fileParentUrlPath = getUrlPath("/singlefile_parquet_dir/");

      ParquetFileConfig fileConfig = new ParquetFileConfig();
      JobDataFragment data =
          expectSuccess(
              getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                  .buildPost(Entity.json(fileConfig)),
              JobDataFragment.class);
      assertEquals(25, data.getReturnedRowCount());
      assertEquals(4, data.getColumns().size());

      try (final JobDataFragment jobData =
          submitJobAndGetData(
              l(JobsService.class),
              sqlQueryRequestFromFile("/singlefile_parquet_dir/0_0_0.parquet"),
              0,
              500,
              allocator)) {
        assertEquals(25, jobData.getReturnedRowCount());
        assertEquals(4, jobData.getColumns().size());
      }
      checkCounts(fileParentUrlPath, "0_0_0.parquet", true, 1, 0, 0);
    } finally {
      resetSystemOption("dac.format.preview.batch_size");
    }
  }

  @Test
  public void testZeroRowParquetFile() throws Exception {
    String fileUrlPath = getUrlPath("/zero-rows/zero-rows.parquet");

    ParquetFileConfig fileConfig = new ParquetFileConfig();
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(0, data.getReturnedRowCount());
    assertEquals(4, data.getColumns().size());
  }

  @Test
  public void testCreateExternalDatasetOnFile() throws Exception {
    submitJobAndWaitUntilCompletion(sqlQueryRequestFromFile("/datasets/csv/comma.csv"));
    String filePath1 = getUrlPath("/datasets/csv/comma.csv");
    String fileParentUrlPath = getUrlPath("/datasets/");

    TextFileConfig format1 =
        (TextFileConfig)
            expectSuccess(
                    getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath1))
                        .buildGet(),
                    FileFormatUI.class)
                .getFileFormat();
    assertNotNull(format1);
    assertEquals(FileType.TEXT, format1.getFileType());

    submitJobAndWaitUntilCompletion(sqlQueryRequestFromFile("/datasets/tab.tsv"));
    filePath1 = getUrlPath("/datasets/tab.tsv");
    format1 =
        (TextFileConfig)
            expectSuccess(
                    getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath1))
                        .buildGet(),
                    FileFormatUI.class)
                .getFileFormat();
    assertNotNull(format1);
    // TODO (Amit H) define separate classes for each type in FileFormatDefinitions
    assertEquals(FileType.TEXT, format1.getFileType());

    checkCounts(fileParentUrlPath, "comma.csv", true, 1, 0, 0);
    checkCounts(fileParentUrlPath, "tab.tsv", true, 1, 0, 0);
  }

  @Test
  public void testQueryOnFolder() throws Exception {
    /*
     * Change the batch_size to 100 from 20 to make sure that all the records in the parquet file are read consistently to avoid flaky issue
     * that only 20 records are read while reading parquet file is slow in preview because there is a time limit for reading in preview.
     * The time limit is 500ms in getData for preview in FormatTools.
     */
    setSystemOption("dac.format.preview.batch_size", "100");
    try {
      ParquetFileConfig fileConfig = new ParquetFileConfig();
      fileConfig.setName("parquet");

      String filePath = getUrlPath("/datasets/folderdataset");
      String fileParentPath = getUrlPath("/datasets/");

      doc("preview data for source folder");
      JobDataFragment data =
          expectSuccess(
              getBuilder(getAPIv2().path("/source/dacfs_test/folder_preview/" + filePath))
                  .buildPost(Entity.json(fileConfig)),
              JobDataFragment.class);
      assertEquals(25, data.getReturnedRowCount());

      expectSuccess(
          getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
              .buildPut(Entity.json(fileConfig)));

      checkCounts(fileParentPath, "folderdataset", true, 1, 0, 0);
    } finally {
      resetSystemOption("dac.format.preview.batch_size");
    }
  }

  @Test
  public void testPreviewWithIgnoreOtherFileFormats() throws Exception {
    ParquetFileConfig fileConfig = new ParquetFileConfig();
    fileConfig.setName("parquet");
    fileConfig.setIgnoreOtherFileFormats(true);

    String filePath = getUrlPath("/datasets/parquet_2p_4s_mixed");

    doc("preview data for source folder");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/folder_preview/" + filePath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(4, data.getReturnedRowCount());

    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
            .buildPut(Entity.json(fileConfig)));
  }

  @Test
  public void listSource() {
    SourceUI source =
        expectSuccess(getBuilder(getAPIv2().path("/source/dacfs_test")).buildGet(), SourceUI.class);
    System.out.println(source.getContents());
  }

  @Test
  public void listFolder() throws Exception {
    doc("list source folder");
    String filePath = getUrlPath("/datasets/text");
    Folder folder =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/folder/" + filePath)).buildGet(),
            Folder.class);
    NamespaceTree ns = folder.getContents();
    assertEquals(0, ns.getFolders().size());
    assertEquals(3, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());

    for (int i = 0; i < 3; i++) {
      assertFalse(ns.getFiles().get(i).isQueryable());
    }

    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setName("comma.txt");

    expectSuccess(
        getBuilder(getAPIv2().path(ns.getFiles().get(0).getLinks().get("format")))
            .buildPut(Entity.json(fileConfig)),
        FileFormatUI.class);

    expectSuccess(
        getBuilder(getAPIv2().path(ns.getFiles().get(1).getLinks().get("format")))
            .buildPut(Entity.json(fileConfig)),
        FileFormatUI.class);

    expectSuccess(
        getBuilder(getAPIv2().path(ns.getFiles().get(2).getLinks().get("format")))
            .buildPut(Entity.json(fileConfig)),
        FileFormatUI.class);

    folder =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/folder/" + filePath)).buildGet(),
            Folder.class);
    ns = folder.getContents();
    assertEquals(0, ns.getFolders().size());
    assertEquals(3, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());
    assertTrue(ns.getFiles().get(0).isQueryable());
    assertNotNull(ns.getFiles().get(0).getId());
    assertEquals(
        0,
        (long)
            ns.getFiles().get(0).getJobCount()); // ui preview queries dont count towards job count
    assertTrue(ns.getFiles().get(1).isQueryable());
    assertNotNull(ns.getFiles().get(1).getId());
    assertEquals(0, (long) ns.getFiles().get(1).getJobCount());
    assertTrue(ns.getFiles().get(2).isQueryable());
    assertNotNull(ns.getFiles().get(2).getId());
    assertEquals(0, (long) ns.getFiles().get(2).getJobCount());
  }

  @Test
  public void testTsvDetect() throws Exception {
    populateInitialData();

    File folder1 = new File(getPopulator().getPath().toFile(), "tmp/_dac2/folderTSV");
    Files.createDirectories(folder1.toPath());
    Files.write(new File(folder1, "file.tsv").toPath(), "a\tf\nc\td".getBytes(UTF_8));
    String folderFormatUrl = "/source/LocalFS1/file_format/tmp/_dac2/folderTSV/file.tsv";
    FileFormatUI defaultFormat =
        expectSuccess(getBuilder(getAPIv2().path(folderFormatUrl)).buildGet(), FileFormatUI.class);
    assertEquals(FileType.TEXT, defaultFormat.getFileFormat().getFileType());
    assertFalse(defaultFormat.getFileFormat().getIsFolder());
    assertEquals(folderFormatUrl, defaultFormat.getLinks().get("self"));
  }

  @Test
  public void testPhysicalDatasetSourceFolders() throws Exception {
    populateInitialData();

    File folder1 = new File(getPopulator().getPath().toFile(), "tmp/_dac/folder1");
    Files.createDirectories(folder1.toPath());
    Files.write(new File(folder1, "file.txt").toPath(), "a|f\nc|d".getBytes(UTF_8));

    File folder2 = new File(getPopulator().getPath().toFile(), "tmp/_dac/folder2");
    Files.createDirectories(folder2.toPath());
    Files.write(new File(folder2, "file.txt").toPath(), "a|f\nc|d".getBytes(UTF_8));

    File folder3 = new File(getPopulator().getPath().toFile(), "tmp/_dac/folder3");
    Files.createDirectories(folder3.toPath());
    Files.write(new File(folder3, "file.txt").toPath(), "a|f\nc|d".getBytes(UTF_8));

    doc("get default format for folder");
    String folderFormatUrl = "/source/LocalFS1/folder_format/tmp/_dac/folder1";
    FileFormatUI defaultFormat =
        expectSuccess(getBuilder(getAPIv2().path(folderFormatUrl)).buildGet(), FileFormatUI.class);
    assertEquals(folderFormatUrl, defaultFormat.getLinks().get("self"));

    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setComment("#");
    fileConfig.setFieldDelimiter("|");
    fileConfig.setName("fff");
    fileConfig.setVersion(null);

    FileFormat fileFormat1 = FileFormat.getForFolder(fileConfig.asFileConfig());
    fileFormat1.setName("tmp._dac.folder1");

    FileFormat fileFormat2 = FileFormat.getForFolder(fileConfig.asFileConfig());
    fileFormat2.setName("tmp._dac.folder2");

    doc("create physical dataset from source folder");
    expectSuccess(
        getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder1"))
            .buildPut(Entity.json(fileFormat1)));

    expectSuccess(
        getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder2"))
            .buildPut(Entity.json(fileFormat2)));

    doc("get physical dataset config from source folder");
    fileFormat1 =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder1"))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(fileFormat1.getName(), fileFormat1.getName());

    fileFormat2 =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder2"))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(fileFormat2.getName(), fileFormat2.getName());

    doc("test updating the format settings of folder dataset");
    {
      fileConfig.setFieldDelimiter(",");
      fileConfig.setVersion(fileFormat1.getVersion());
      fileFormat1 = FileFormat.getForFolder(fileConfig.asFileConfig());

      expectSuccess(
          getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder1"))
              .buildPut(Entity.json(fileFormat1)));

      // retrieve the format back and check the updates are present.
      fileFormat1 =
          expectSuccess(
                  getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder1"))
                      .buildGet(),
                  FileFormatUI.class)
              .getFileFormat();

      assertTrue(fileFormat1 instanceof TextFileConfig);
      assertEquals(",", ((TextFileConfig) fileFormat1).getFieldDelimiter());
    }

    doc("list source folder and see if folder1 and folder2 are marked as physical dataset");
    Folder folder =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/LocalFS1/folder/tmp/_dac")).buildGet(),
            Folder.class);
    NamespaceTree ns = folder.getContents();
    assertEquals(3, ns.getFolders().size());
    assertEquals(0, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());

    for (Folder f : ns.getFolders()) {
      if ("folder1".equals(f.getName())) {
        assertTrue(f.isQueryable());
      } else if ("folder2".equals(f.getName())) {
        assertTrue(f.isQueryable());
      } else if ("folder3".equals(f.getName())) {
        assertFalse(f.isQueryable());
      } else {
        fail("Invalid folder found " + f.getFullPathList());
      }
    }

    doc("delete with bad version");
    long badVersion = 1234L;
    String expectedErrorMessage =
        String.format(
            "Cannot delete folder format \"%s\", version provided \"%s\" is different from version found \"%s\"",
            "tmp/_dac/folder1", badVersion, fileFormat1.getVersion());
    final GenericErrorMessage errorDelete2 =
        expectStatus(
            CONFLICT,
            getBuilder(
                    getAPIv2()
                        .path("/source/LocalFS1/folder_format/tmp/_dac/folder1")
                        .queryParam("version", badVersion))
                .buildDelete(),
            GenericErrorMessage.class);
    assertThat(errorDelete2.getErrorMessage()).isEqualTo(expectedErrorMessage);

    doc("delete physical dataset for source folder");
    expectSuccess(
        getBuilder(
                getAPIv2()
                    .path("/source/LocalFS1/folder_format/tmp/_dac/folder1")
                    .queryParam("version", fileFormat1.getVersion()))
            .buildDelete());

    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/LocalFS1/folder_format/tmp/_dac/folder1"))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.TEXT, fileFormat.getFileType());

    folder =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/LocalFS1/folder/tmp/_dac")).buildGet(),
            Folder.class);
    ns = folder.getContents();
    assertEquals(3, ns.getFolders().size());
    assertEquals(0, ns.getFiles().size());
    assertEquals(0, ns.getPhysicalDatasets().size());

    for (Folder f : ns.getFolders()) {
      if ("folder1".equals(f.getName())) {
        assertFalse(f.isQueryable());
      } else if ("folder2".equals(f.getName())) {
        assertTrue(f.isQueryable());
      } else if ("folder3".equals(f.getName())) {
        assertFalse(f.isQueryable());
      } else {
        fail("Invalid folder found " + f.getFullPathList());
      }
    }
  }

  @Test
  public void testExcelWithHeaderAndMergeCellExpansion() throws Exception {
    Invocation inv =
        getExcelTestQueryInvocation(getUrlPath("/testfiles/excel.xlsx"), "sheet 1", true, true);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(5, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testXlsWithHeaderAndMergeCellExpansion() throws Exception {
    Invocation inv =
        getXlsTestQueryInvocation(getUrlPath("/testfiles/excel.xls"), "sheet 1", true, true);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(5, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testExcelWithHeaderAndNoMergeCellExpansion() throws Exception {
    Invocation inv =
        getExcelTestQueryInvocation(getUrlPath("/testfiles/excel.xlsx"), "sheet 1", true, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(5, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testXlsWithHeaderAndNoMergeCellExpansion() throws Exception {
    Invocation inv =
        getXlsTestQueryInvocation(getUrlPath("/testfiles/excel.xls"), "sheet 1", true, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(5, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testExcelNoWithHeaderAndNoMergeCellExpansion() throws Exception {
    Invocation inv =
        getExcelTestQueryInvocation(getUrlPath("/testfiles/excel.xlsx"), "Sheet 1", false, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(6, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testXlsNoWithHeaderAndNoMergeCellExpansion() throws Exception {
    Invocation inv =
        getXlsTestQueryInvocation(getUrlPath("/testfiles/excel.xls"), "Sheet 1", false, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(6, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testExcelNullSheetName() throws Exception {
    Invocation inv =
        getExcelTestQueryInvocation(getUrlPath("/testfiles/excel.xlsx"), null, false, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(6, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  @Test
  public void testXlsNullSheetName() throws Exception {
    Invocation inv =
        getXlsTestQueryInvocation(getUrlPath("/testfiles/excel.xls"), null, false, false);
    JobDataFragment data = expectSuccess(inv, JobDataFragment.class);
    assertEquals(6, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());
  }

  private Invocation getExcelTestQueryInvocation(
      String filePath, String sheet, boolean extractHeader, boolean hasMergedCells) {
    ExcelFileConfig fileConfig = new ExcelFileConfig();
    fileConfig.setSheetName(sheet);
    if (extractHeader) {
      fileConfig.setExtractHeader(true);
    } // false is the default value
    if (hasMergedCells) {
      fileConfig.setHasMergedCells(true);
    } // false is the default value

    return getBuilder(getAPIv2().path("/source/dacfs_test/file_preview" + filePath))
        .buildPost(Entity.json(fileConfig));
  }

  private Invocation getXlsTestQueryInvocation(
      String filePath, String sheet, boolean extractHeader, boolean hasMergedCells) {
    XlsFileConfig fileConfig = new XlsFileConfig();
    fileConfig.setSheetName(sheet);
    if (extractHeader) {
      fileConfig.setExtractHeader(true);
    } // false is the default value
    if (hasMergedCells) {
      fileConfig.setHasMergedCells(true);
    } // false is the default value

    return getBuilder(getAPIv2().path("/source/dacfs_test/file_preview" + filePath))
        .buildPost(Entity.json(fileConfig));
  }

  // Based off data created by TestParquetMetadataCache:testCache
  @Test
  public void testQueryOnParquetDirWithMetadata() throws Exception {
    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class), sqlQueryRequestFromFile("/nation_ctas"), 0, 500, allocator)) {
      assertEquals(50, jobData.getReturnedRowCount());
      // extra column for "dir" (t1 and t2 are directories under nation_ctas)
      assertEquals(5, jobData.getColumns().size());
    }
  }

  @Test
  public void testQueryOnParquetDirWithSingleFile() throws Exception {
    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/singlefile_parquet_dir"),
            0,
            500,
            allocator)) {
      assertEquals(25, jobData.getReturnedRowCount());
      assertEquals(4, jobData.getColumns().size());
    }
  }

  @Test
  public void testDefaultFileFormatForParquetFolder() throws Exception {
    doc("get physical dataset config from source folder");
    String folderPath = getUrlPath("/singlefile_parquet_dir");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + folderPath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.PARQUET, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForTextFolder() throws Exception {
    String folderPath = getUrlPath("/datasets/text");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + folderPath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.TEXT, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForCsvFolder() throws Exception {
    String folderPath = getUrlPath("/datasets/csv");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + folderPath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.TEXT, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForJsonFolder() throws Exception {
    String folderPath = getUrlPath("/json");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + folderPath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.JSON, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForParquetFile() throws Exception {
    String filePath = getUrlPath("/singlefile_parquet_dir/0_0_0.parquet");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.PARQUET, fileFormat.getFileType());
  }

  @Test
  public void testCompressedGZ() throws Exception {
    String filePath = getUrlPath("/datasets/compressed/test.json.gz");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.JSON, fileFormat.getFileType());
  }

  @Test
  public void testCompressedZip() throws Exception {
    String filePath = getUrlPath("/datasets/compressed/test.json.zip");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.JSON, fileFormat.getFileType());
  }

  @Test
  public void testIgnoreHiddenFiles() throws Exception {
    String filePath = getUrlPath("/datasets/folderdataset/");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.PARQUET, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForCsvFile() throws Exception {
    String filePath = getUrlPath("/datasets/csv/comma.csv");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.TEXT, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForTextFile() throws Exception {
    String filePath = getUrlPath("/datasets/text/comma.txt");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.TEXT, fileFormat.getFileType());
  }

  @Test
  public void testDefaultFileFormatForJsonFile() throws Exception {
    String filePath = getUrlPath("/json/mixed.json");
    FileFormat fileFormat =
        expectSuccess(
                getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + filePath))
                    .buildGet(),
                FileFormatUI.class)
            .getFileFormat();
    assertEquals(FileType.JSON, fileFormat.getFileType());
  }

  @Test
  public void testPreviewTinyAcqWithHeader() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter("|");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("tinyacq.txt");
    fileConfig.setExtractHeader(true);

    String fileUrlPath = getUrlPath("/datasets/tinyacq.txt");

    doc("preview data for source file");
    JobDataFragment data =
        expectSuccess(
            getBuilder(getAPIv2().path("/source/dacfs_test/file_preview/" + fileUrlPath))
                .buildPost(Entity.json(fileConfig)),
            JobDataFragment.class);
    assertEquals(23, data.getColumns().size());
    // We should get at most FormatTools.TARGET_RECORDS results - we can get less results if the
    // system is busy for
    // example but we should get some results at least.
    final long targetRecords = getOptionManager().getOption(FormatTools.TARGET_RECORDS);
    assertTrue(data.getReturnedRowCount() > 0 && targetRecords >= data.getReturnedRowCount());
  }

  @Test
  public void testQueryTinyAcqWithHeader() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter("|");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("tinyacq.txt");
    fileConfig.setExtractHeader(true);

    String fileUrlPath = getUrlPath("/datasets/tinyacq.txt");

    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/file_format/" + fileUrlPath))
            .buildPut(Entity.json(fileConfig)));
    try (final JobDataFragment jobData =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/datasets/tinyacq.txt"),
            0,
            500,
            allocator)) {
      assertEquals(23, jobData.getColumns().size());
      assertEquals(500, jobData.getReturnedRowCount());
    }
  }

  private void setLastModified(final String filePath, final long currentTime) {
    File file = new File(filePath);
    file.setLastModified(currentTime);
  }

  @Test
  public void testParquetPartitionChunkCount() throws Exception {
    ParquetFileConfig fileConfig = new ParquetFileConfig();
    fileConfig.setName("parquet");
    String filePath = getUrlPath("/datasets/parquet_2p_4s");
    final long currentTime = now();
    setLastModified(filePath + "/2020-01-01/1_0_0.parquet", currentTime);
    setLastModified(filePath + "/2020-01-01/1_0_1.parquet", currentTime);
    setLastModified(filePath + "/2020-01-02/1_0_0.parquet", currentTime);
    setLastModified(filePath + "/2020-01-02/1_0_1.parquet", currentTime);

    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
            .buildPut(Entity.json(fileConfig)));

    int expectedNumOfPartitionChunks = 1;
    int expectedNumOfSplitsPerPartition = 1;
    DatasetConfig datasetConfig =
        l(NamespaceService.class)
            .getDataset(new DatasetPath(getSchemaPath("/datasets/parquet_2p_4s")).toNamespaceKey());
    Iterator<PartitionChunkMetadata> iter =
        l(NamespaceService.class)
            .findSplits(
                new ImmutableFindByCondition.Builder()
                    .setCondition(PartitionChunkId.getSplitsQuery(datasetConfig))
                    .build())
            .iterator();
    for (int i = 0; i < expectedNumOfPartitionChunks; i++) {
      assertTrue(iter.hasNext());
      PartitionChunkMetadata partitionChunkMetadata = iter.next();
      logger.debug(
          "Normalized partition info is {}.", partitionChunkMetadata.getNormalizedPartitionInfo());
      for (PartitionProtobuf.DatasetSplit datasetSplit :
          partitionChunkMetadata.getDatasetSplits()) {
        logger.debug("Dataset split is {}.", datasetSplit);
      }
      assertEquals(expectedNumOfSplitsPerPartition, partitionChunkMetadata.getSplitCount());
    }
    assertFalse(iter.hasNext());
  }

  @Test
  public void testTextPartitionChunkCount() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter("|");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setName("tinyacq.txt");
    fileConfig.setExtractHeader(true);

    String filePath = getUrlPath("/datasets/text_2p_4s");
    final long currentTime = now();
    setLastModified(filePath + "/2020-01-01/1.txt", currentTime);
    setLastModified(filePath + "/2020-01-01/2.txt", currentTime);
    setLastModified(filePath + "/2020-01-02/1.txt", currentTime);
    setLastModified(filePath + "/2020-01-02/2.txt", currentTime);

    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
            .buildPut(Entity.json(fileConfig)));

    int expectedNumOfPartitionChunks = 2;
    int expectedNumOfSplitsPerPartition = 2;
    DatasetConfig datasetConfig =
        l(NamespaceService.class)
            .getDataset(new DatasetPath(getSchemaPath("/datasets/text_2p_4s")).toNamespaceKey());
    Iterator<PartitionChunkMetadata> iter =
        l(NamespaceService.class)
            .findSplits(
                new ImmutableFindByCondition.Builder()
                    .setCondition(PartitionChunkId.getSplitsQuery(datasetConfig))
                    .build())
            .iterator();
    for (int i = 0; i < expectedNumOfPartitionChunks; i++) {
      assertTrue(iter.hasNext());
      PartitionChunkMetadata partitionChunkMetadata = iter.next();
      logger.debug(
          "Normalized partition info is {}.", partitionChunkMetadata.getNormalizedPartitionInfo());
      for (PartitionProtobuf.DatasetSplit datasetSplit :
          partitionChunkMetadata.getDatasetSplits()) {
        logger.debug("Dataset split is {}.", datasetSplit);
      }
      assertEquals(expectedNumOfSplitsPerPartition, partitionChunkMetadata.getSplitCount());
    }
    assertFalse(iter.hasNext());
  }

  @Test
  public void testSchemaChangeNewColumn() throws Exception {
    TextFileConfig fileConfig = new TextFileConfig();
    fileConfig.setFieldDelimiter(",");
    fileConfig.setLineDelimiter("\n");
    fileConfig.setTrimHeader(true);
    fileConfig.setExtractHeader(true);

    String filePath = getUrlPath("/datasets/schemaevolution");
    final long currentTime = now();
    setLastModified(filePath + "/2021-01-01/2021-01-01.csv", currentTime);
    setLastModified(filePath + "/2022-02-02/2022-02-02.csv", currentTime);
    setLastModified(filePath + "/2023-03-03/2023-03-03.csv", currentTime);

    expectSuccess(
        getBuilder(getAPIv2().path("/source/dacfs_test/folder_format/" + filePath))
            .buildPut(Entity.json(fileConfig)));

    doc("ensure select * works");
    try (final JobDataFragment jobData1 =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromFile("/datasets/schemaevolution"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData1.getReturnedRowCount());
      // There are 4 columns because of the directory column
      assertEquals(4, jobData1.getColumns().size());
    }

    doc("ensure select NEW_col3 doesn't throw ArrayIndexOutOfBoundsException");
    try (final JobDataFragment jobData2 =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromColumnAndFile("NEW_col3", "/datasets/schemaevolution"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData2.getReturnedRowCount());
      assertEquals(1, jobData2.getColumns().size());
    }

    doc("ensure select OG_col1, OG_col2 works");
    try (final JobDataFragment jobData3 =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromColumnAndFile("OG_col1, OG_col2", "/datasets/schemaevolution"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData3.getReturnedRowCount());
      assertEquals(2, jobData3.getColumns().size());
    }

    doc("ensure select OG_col2, NEW_col3 works");
    try (final JobDataFragment jobData4 =
        submitJobAndGetData(
            l(JobsService.class),
            sqlQueryRequestFromColumnAndFile("OG_col2, NEW_col3", "/datasets/schemaevolution"),
            0,
            500,
            allocator)) {
      assertEquals(3, jobData4.getReturnedRowCount());
      assertEquals(2, jobData4.getColumns().size());
    }
  }
}
