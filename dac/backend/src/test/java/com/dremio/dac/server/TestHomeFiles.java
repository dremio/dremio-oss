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
package com.dremio.dac.server;

import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.util.FileUtils;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;
import com.dremio.dac.explore.model.FileFormatUI;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.homefiles.HomeFileConfig;
import com.dremio.dac.homefiles.HomeFileSystemPluginConfig;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.spaces.Home;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.file.File;
import com.dremio.file.FilePath;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ExcelFileConfig;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.file.proto.XlsFileConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Test home files.
 */
public class TestHomeFiles extends BaseTestServer {
  private static final String HOME_NAME =
      HomeName.getUserHomePath(SampleDataPopulator.DEFAULT_USER_NAME).getName();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private FileSystem fs;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    getPopulator().populateTestUsers();
    this.fs = l(HomeFileConfig.class).createFileSystem();
  }

  private void checkFileData(String location) throws Exception {
    Path serverFileDirPath = new Path(location);
    assertTrue(fs.exists(serverFileDirPath));
    FileStatus[] statuses = fs.listStatus(serverFileDirPath);
    assertEquals(1, statuses.length);

    int fileSize = (int)statuses[0].getLen();
    final byte[] data = new byte[fileSize];
    FSDataInputStream inputStream = fs.open(statuses[0].getPath());
    org.apache.hadoop.io.IOUtils.readFully(inputStream, data, 0, fileSize);
    inputStream.close();
    assertEquals("{\"person_id\": 1, \"salary\": 10}", new String(data));
  }

  private void checkFileDoesNotExist(String location) throws Exception {
    Path serverFileDirPath = new Path(location);
    assertFalse(fs.exists(serverFileDirPath));
  }

  @Test
  public void testHome() throws Exception {
    final JobsService jobsService = l(JobsService.class);

    Home home = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME)).buildGet(), Home.class);
    assertNotNull(home.getId());

    java.io.File inputFile = temporaryFolder.newFile("input.json");
    try(FileWriter fileWriter = new FileWriter(inputFile)) {
      fileWriter.write("{\"person_id\": 1, \"salary\": 10}");
    }

    FormDataMultiPart form = new FormDataMultiPart();
    FormDataBodyPart fileBody = new FormDataBodyPart("file", inputFile, MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);
    doc("upload file to staging");
    File file1Staged = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_start/file1").queryParam("extension", "json")).buildPost(
      Entity.entity(form, form.getMediaType())), File.class);
    FileFormat file1StagedFormat = file1Staged.getFileFormat().getFileFormat();
    assertEquals("file1", file1StagedFormat.getName());
    assertEquals(asList(HOME_NAME, "file1"), file1StagedFormat.getFullPath());
    assertEquals(FileType.JSON, file1StagedFormat.getFileType());

    fileBody.cleanup();

    checkFileData(file1StagedFormat.getLocation());

    // external query
    String fileLocation = PathUtils.toDottedPath(new org.apache.hadoop.fs.Path(file1StagedFormat.getLocation()));
    SqlQuery query = new SqlQuery(format("select * from table(%s.%s (%s)) limit 500",
      SqlUtils.quoteIdentifier(HomeFileConfig.HOME_PLUGIN_NAME), fileLocation, file1StagedFormat.toTableOptions()), SampleDataPopulator.DEFAULT_USER_NAME);

    doc("querying file");
    JobUI job = new JobUI(jobsService.submitExternalJob(query, QueryType.UI_PREVIEW));
    job.getData().loadIfNecessary();
    JobDataFragment truncData = job.getData().truncate(500);
    assertEquals(1, truncData.getReturnedRowCount());
    assertEquals(2, truncData.getColumns().size());

    doc("previewing staged file");
    JobDataFragment data = expectSuccess(getBuilder(getAPIv2().path("/home/" + HOME_NAME + "/file_preview_unsaved/file1")).buildPost(Entity.json(file1StagedFormat)), JobDataFragment.class);
    assertEquals(1, data.getReturnedRowCount());
    assertEquals(2, data.getColumns().size());

    // finish upload
    File file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_finish/file1")).buildPost(Entity.json(file1StagedFormat)), File.class);
    FileFormat file1Format = file1.getFileFormat().getFileFormat();
    assertEquals("file1", file1Format.getName());
    assertEquals(asList(HOME_NAME, "file1"), file1Format.getFullPath());
    assertEquals(FileType.JSON, file1Format.getFileType());

    checkFileData(file1Format.getLocation());
    checkFileDoesNotExist(file1StagedFormat.getLocation());

    // test upload cancel
    form = new FormDataMultiPart();
    fileBody = new FormDataBodyPart("file", inputFile, MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);

    doc("upload second file to staging");
    File file2Staged = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_start/file2").queryParam("extension", "json")).buildPost(
      Entity.entity(form, form.getMediaType())), File.class);
    FileFormat file2StagedFormat = file2Staged.getFileFormat().getFileFormat();

    assertEquals("file2", file2StagedFormat.getName());
    assertEquals(asList(HOME_NAME, "file2"), file2StagedFormat.getFullPath());
    assertEquals(FileType.JSON, file2StagedFormat.getFileType());

    fileBody.cleanup();
    checkFileData(file2StagedFormat.getLocation());

    // cancel upload
    doc("cancel upload for second file");
    expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_cancel/file2")).buildPost(Entity.json(file2StagedFormat)));
    checkFileDoesNotExist(file2StagedFormat.getLocation());
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file/file2")).buildGet(), NotFoundErrorMessage.class);

    doc("getting a file");
    File file2 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file/file1")).buildGet(), File.class);
    FileFormat file2Format = file2.getFileFormat().getFileFormat();

    assertEquals("file1", file2Format.getName());
    assertEquals(asList(HOME_NAME, "file1"), file2Format.getFullPath());
    assertEquals(FileType.JSON, file2Format.getFileType());

    doc("querying file");
    job = new JobUI(jobsService.submitExternalJob(new SqlQuery("select * from \"" + HOME_NAME + "\".file1", SampleDataPopulator.DEFAULT_USER_NAME), com.dremio.service.job.proto.QueryType.UNKNOWN));
    job.getData().loadIfNecessary();
    truncData = job.getData().truncate(500);
    assertEquals(1, truncData.getReturnedRowCount());
    assertEquals(2, truncData.getColumns().size());

    doc("creating dataset from home file");
    InitialPreviewResponse response = expectSuccess(getBuilder(getAPIv2().path(
      "/home/" + HOME_NAME + "/new_untitled_from_file/file1")).buildPost(Entity.json("")), InitialPreviewResponse.class);
    assertEquals(2, response.getData().getColumns().size());

    doc("renaming file");
    File file3 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file_rename/file1").queryParam("renameTo", "file1r"))
      .buildPost(Entity.json(new FileConfig())), File.class);
    FileFormat file3Format = file3.getFileFormat().getFileFormat();

    assertEquals("file1r", file3Format.getName());
    assertEquals(asList(HOME_NAME, "file1r"), file3Format.getFullPath());
    assertEquals(FileType.JSON, file3Format.getFileType());

    expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file/file1r")).buildGet(), File.class);
    expectError(CLIENT_ERROR, getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file/file1")).buildGet(), NotFoundErrorMessage.class);

    Home home1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME)).buildGet(), Home.class);
    assertEquals(1, home1.getContents().getFiles().size());

    doc("creating a folder");
    String folderPath = "home/" + HOME_NAME + "/folder/";

    final Folder putFolder1 = expectSuccess(getBuilder(getAPIv2().path(folderPath)).buildPost(Entity.json("{\"name\": \"f1\"}")), Folder.class);
    assertEquals("f1", putFolder1.getName());

    doc("get folder");
    Folder f1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/folder/f1")).buildGet(), Folder.class);
    assertEquals("f1", f1.getName());
    Assert.assertArrayEquals(new String[]{HOME_NAME, "f1"}, f1.getFullPathList().toArray());
  }

  @Test // DX-5410
  public void formatChangeForUploadedHomeFile() throws Exception {
    FormDataMultiPart form = new FormDataMultiPart();
    FormDataBodyPart fileBody = new FormDataBodyPart("file", FileUtils.getResourceAsFile("/datasets/csv/pipe.csv"), MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);

    doc("uploading a text file");
    File file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_start/pipe").queryParam("extension", "csv"))
        .buildPost(Entity.entity(form, form.getMediaType())), File.class);
    file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_finish/pipe"))
        .buildPost(Entity.json(file1.getFileFormat().getFileFormat())), File.class);
    final FileFormat defaultFileFormat = file1.getFileFormat().getFileFormat();

    assertTrue(defaultFileFormat instanceof TextFileConfig);
    assertEquals(",", ((TextFileConfig)defaultFileFormat).getFieldDelimiter());

    doc("change the format settings of uploaded file");
    final TextFileConfig newFileFormat = (TextFileConfig)defaultFileFormat;
    newFileFormat.setFieldDelimiter("|");

    final FileFormat updatedFileFormat = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file_format/pipe"))
        .buildPut(Entity.json(newFileFormat)), FileFormatUI.class).getFileFormat();
    assertTrue(updatedFileFormat instanceof TextFileConfig);
    assertEquals("|", ((TextFileConfig)updatedFileFormat).getFieldDelimiter());
  }

  @Test
  public void testUploadXlsxFile() throws Exception {
    testUploadExcelFile(false);
  }

  @Test
  public void testUploadXlsFile() throws Exception {
    testUploadExcelFile(true);
  }

  private void testUploadExcelFile(final boolean isXLS) throws Exception {
    final String extension = isXLS ? "xls" : "xlsx";
    final FileType fileType = isXLS ? FileType.XLS : FileType.EXCEL;

    FormDataMultiPart form = new FormDataMultiPart();
    FormDataBodyPart fileBody = new FormDataBodyPart("file", FileUtils.getResourceAsFile("/testfiles/excel." + extension), MediaType.MULTIPART_FORM_DATA_TYPE);
    form.bodyPart(fileBody);

    doc("uploading excel file");
    File file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_start/excel").queryParam("extension", extension)).buildPost(
      Entity.entity(form, form.getMediaType())), File.class);
    file1 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/upload_finish/excel")).buildPost(
        Entity.json(file1.getFileFormat().getFileFormat())), File.class);
    FileFormat file1Format = file1.getFileFormat().getFileFormat();

    assertEquals("excel", file1Format.getName());
    assertEquals(asList(HOME_NAME, "excel"), file1Format.getFullPath());
    assertEquals(fileType, file1Format.getFileType());

    fileBody.cleanup();

    doc("getting a excel file");
    File file2 = expectSuccess(getBuilder(getAPIv2().path("home/" + HOME_NAME + "/file/excel")).buildGet(), File.class);
    FileFormat file2Format = file2.getFileFormat().getFileFormat();

    assertEquals("excel", file2Format.getName());
    assertEquals(asList(HOME_NAME, "excel"), file2Format.getFullPath());
    assertEquals(fileType, file2Format.getFileType());

    doc("querying excel file");
    final JobsService jobsService = l(JobsService.class);
    JobUI job = new JobUI(jobsService.submitExternalJob(new SqlQuery("select * from \"" + HOME_NAME + "\".\"excel\"", SampleDataPopulator.DEFAULT_USER_NAME), com.dremio.service.job.proto.QueryType.UNKNOWN));
    job.getData().loadIfNecessary();
    JobDataFragment truncData = job.getData().truncate(500);
    assertEquals(6, truncData.getReturnedRowCount());
    assertEquals(5, truncData.getColumns().size());

    doc("previewing excel file");
    if (file2Format instanceof ExcelFileConfig) {
      ((ExcelFileConfig) file2Format).setExtractHeader(true);
    } else {
      ((XlsFileConfig) file2Format).setExtractHeader(true);
    }
    JobDataFragment data = expectSuccess(getBuilder(getAPIv2().path("/home/" + HOME_NAME + "/file_preview/excel")).buildPost(Entity.json(file2Format)), JobDataFragment.class);
    assertEquals(5, data.getReturnedRowCount());
    assertEquals(5, data.getColumns().size());

    doc("creating dataset from excel file");
    InitialPreviewResponse previewResponse = expectSuccess(getBuilder(getAPIv2().path(
      "/home/" + HOME_NAME + "/new_untitled_from_file/excel")).buildPost(Entity.json("")), InitialPreviewResponse.class);
    assertEquals(5, previewResponse.getData().getColumns().size());
  }

  public static void uploadFile(HomeFileConfig homeFileStore, Path inputFile, String name, String extension ,FileFormat fileFormat, FolderPath parent) throws Exception {
    FilePath filePath;
    if (parent == null) {
      filePath = new FilePath(ImmutableList.of(HomeName.getUserHomePath(DEFAULT_USER_NAME).getName(), name));
    } else {
      List<String> path = Lists.newArrayList(parent.toPathList());
      path.add(name);
      filePath = new FilePath(path);
    }

    FSDataInputStream inputStream = FileSystem.getLocal(new Configuration()).open(inputFile);
    FileSystem fs = homeFileStore.createFileSystem();
    Path stagingLocation = new HomeFileTool(homeFileStore, fs).stageFile(filePath, extension, inputStream);
    Path finalLocation = new HomeFileTool(homeFileStore, fs).saveFile(stagingLocation, filePath, extension);
    inputStream.close();

    // create file in namespace

    fileFormat.setFullPath(filePath.toPathList());
    fileFormat.setName(name);
    fileFormat.setLocation(finalLocation.toString());
    DatasetConfig datasetConfig = DatasetsUtil.toDatasetConfig(fileFormat.asFileConfig(), DatasetType.PHYSICAL_DATASET_HOME_FILE, null, null);
    newCatelogService().createOrUpdateDataset(newNamespaceService(), new NamespaceKey(HomeFileConfig.HOME_PLUGIN_NAME), filePath.toNamespaceKey(), datasetConfig);
  }

  public static void runQuery(String name, int rows, int columns, FolderPath parent) {
    FilePath filePath;
    if (parent == null) {
      filePath = new FilePath(ImmutableList.of(HomeName.getUserHomePath(DEFAULT_USER_NAME).getName(), name));
    } else {
      List<String> path = Lists.newArrayList(parent.toPathList());
      path.add(name);
      filePath = new FilePath(path);
    }
    final JobsService jobsService = l(JobsService.class);
    JobUI job = new JobUI(jobsService.submitExternalJob(new SqlQuery(format("select * from %s", filePath.toPathString()), DEFAULT_USER_NAME), com.dremio.service.job.proto.QueryType.UNKNOWN));
    JobDataFragment truncData = job.getData().truncate(rows + 1);
    assertEquals(rows, truncData.getReturnedRowCount());
    assertEquals(columns, truncData.getColumns().size());
  }

  private static void runTests(HomeFileConfig homeFileStore) throws Exception {
    // text file
    Path textFile = new Path(FileUtils.getResourceAsFile("/datasets/text/comma.txt").getAbsolutePath());
    uploadFile(homeFileStore, textFile, "comma", "txt", new TextFileConfig().setFieldDelimiter(","), null);

    Path csvFile = new Path(FileUtils.getResourceAsFile("/datasets/csv/comma.csv").getAbsolutePath());
    uploadFile(homeFileStore, csvFile, "comma1", "csv", new TextFileConfig().setFieldDelimiter(","), null);

    Path jsonFile = new Path(FileUtils.getResourceAsFile("/datasets/users.json").getAbsolutePath());
    uploadFile(homeFileStore, jsonFile, "users", "json", new JsonFileConfig(), null);

    Path excelFile = new Path(FileUtils.getResourceAsFile("/testfiles/excel.xlsx").getAbsolutePath());
    uploadFile(homeFileStore, excelFile, "excel", "xlsx", new ExcelFileConfig(), null);

    // query files
    runQuery("comma", 4, 3, null);
    runQuery("comma1", 4, 3, null);
    runQuery("users", 3, 2, null);
    runQuery("excel", 6, 5, null);

    // add file to folder
    FolderPath folderPath = new FolderPath(ImmutableList.of(HomeName.getUserHomePath(DEFAULT_USER_NAME).getName(), "testupload"));
    newNamespaceService().addOrUpdateFolder(folderPath.toNamespaceKey(), new FolderConfig()
      .setName("testupload")
      .setFullPathList(folderPath.toPathList()));

    uploadFile(homeFileStore, textFile, "comma", "txt", new TextFileConfig().setFieldDelimiter(","), folderPath);
    runQuery("comma", 4, 3, folderPath);

  }

  @Test
  public void testNASFileStore() throws Exception {
    final HomeFileConfig nasHomeFileStore = new HomeFileConfig(new Path("file://" + BaseTestServer.folder1.getRoot().toString() + "/" + "testNASFileStore").toUri(), "localhost");
    HomeFileTool tool = new HomeFileTool(nasHomeFileStore, nasHomeFileStore.createFileSystem());
    StoragePluginRegistry registry = getCurrentDremioDaemon().getBindingProvider().lookup(StoragePluginRegistry.class);
    registry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME, new HomeFileSystemPluginConfig(nasHomeFileStore), true);

    try {
      runTests(nasHomeFileStore);
    } finally {
      tool.clear();
      // reset plugin
      final HomeFileConfig pdfsHomeFileStore = l(HomeFileConfig.class);
      registry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME, new HomeFileSystemPluginConfig(pdfsHomeFileStore), true);
    }
  }


  @Test
  public void testPDFSFileStore() throws Exception {
    final HomeFileConfig pdfsHomeFileStore = l(HomeFileConfig.class);
    HomeFileTool tool = new HomeFileTool(pdfsHomeFileStore, pdfsHomeFileStore.createFileSystem());
    try {
      runTests(pdfsHomeFileStore);
    } finally {
      tool.clear();
    }
  }

  @Test
  @Ignore("This test connects to hdfs at h011.d.drem.io:8020")
  public void testHDFSFileStore() throws Exception {
    final HomeFileConfig hdfsHomeFileStore = new HomeFileConfig(new Path("hdfs://h011.d.drem.io:8020/tmp/" + BaseTestServer.folder1.getRoot().toString() + "/" + "testNASFileStore").toUri(), "localhost");
    HomeFileTool tool = new HomeFileTool(hdfsHomeFileStore, hdfsHomeFileStore.createFileSystem());
    StoragePluginRegistry registry = getCurrentDremioDaemon().getBindingProvider().lookup(StoragePluginRegistry.class);
    registry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME, new HomeFileSystemPluginConfig(hdfsHomeFileStore), true);
    try {
      runTests(hdfsHomeFileStore);
    } finally {
      tool.clear();
      // reset plugin?
      final HomeFileConfig pdfsHomeFileStore = l(HomeFileConfig.class);
      registry.createOrUpdate(HomeFileConfig.HOME_PLUGIN_NAME, new HomeFileSystemPluginConfig(pdfsHomeFileStore), true);
    }
  }
}
