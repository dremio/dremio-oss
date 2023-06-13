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
package com.dremio.dac.server;

import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_END;
import static com.dremio.dac.proto.model.dataset.Direction.FROM_THE_START;
import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.dac.proto.model.dataset.OrderDirection.DESC;
import static java.util.Arrays.asList;
import static javax.ws.rs.client.Entity.entity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.ExtractPreviewReq;
import com.dremio.dac.explore.model.HistoryItem;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.explore.model.extract.ExtractCards;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.ExtractCard;
import com.dremio.dac.proto.model.dataset.IndexType;
import com.dremio.dac.proto.model.dataset.Offset;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.TrimType;
import com.dremio.dac.service.source.SourceService;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.job.proto.JobState;

/**
 * Server test with initial data
 */
public class TestServerWithInitialData extends BaseTestServer {

  private static boolean populated = false;

  private void initData() throws Exception {
    if (!populated) {
      clearAllDataExceptUser();
      populateInitialData();
      populated = true;
    }
  }

  private InitialPreviewResponse transformAndValidate(DatasetUI versionedDataset, TransformBase transform) {
    InitialPreviewResponse transformResponse = transform(versionedDataset, transform);

    String initialResults = transformResponse.getData().toString();
    assertTrue(initialResults, transformResponse.getData().getColumns().size() > 0);
    assertTrue(initialResults, transformResponse.getData().getReturnedRowCount() > 0);

    return transformResponse;
  }

  @Test
  public void testSortExistingDataset() throws Exception {
    initData();
    doc("get spaces");
    expectSuccess(getBuilder(getAPIv2().path("space/Sales-Sample")).buildGet(), Space.class);

    final DatasetUI dsGet = getDataset(new DatasetPath("Sales-Sample.ds4"));

    doc("sort dataset by B");
    // transform and save
    DatasetUI dsTransform1 = transformAndValidate(dsGet, new TransformSort("B", ASC)).getDataset();
    assertContains("order by b asc", dsTransform1.getSql().toLowerCase());

    doc("sort multiple columns");
    DatasetUI dsTransform2 = transformAndValidate(dsGet,
        new TransformSorts().setColumnsList(asList(new Order("B", ASC), new Order("A", DESC)))).getDataset();
    assertContains("order by b asc, a desc", dsTransform2.getSql().toLowerCase());
  }

  @Test
  public void testDropColExistingDataset() throws Exception {
    initData();

    final DatasetUI dsGet = getDataset(new DatasetPath("Prod-Sample.ds1"));

    doc("sort dataset by B");
    transformAndValidate(dsGet, new TransformDrop("age"));
  }

  @Test
  public void testSort2ExistingDataset() throws Exception {
    initData();

    final DatasetUI dsGet = getDataset(new DatasetPath("Prod-Sample.ds1"));

    doc("sort dataset by B");
    transformAndValidate(dsGet, new TransformSort().setSortedColumnName("age"));
  }


  @Test
  public void testRenameColExistingDataset() throws Exception {
    initData();

    final DatasetUI dsGet = getDataset(new DatasetPath("Sales-Sample.ds3"));

    doc("rename col B");
    transformAndValidate(dsGet, new TransformRename("B", "asdasdasB"));
  }

  @Test
  public void testTrimExistingDataset() throws Exception {
    setSpace();

    DatasetPath datasetPath = new DatasetPath("spacefoo.folderbar.folderbaz.trimexisting");
    createDatasetFromParentAndSave(datasetPath, "cp.\"json/stringCaseAndTrim.json\"");
    final DatasetUI dsGet = getDataset(datasetPath);

    doc("trim col B");
    transformAndValidate(dsGet, new TransformTrim("a", TrimType.BOTH, "C", false));
    transformAndValidate(dsGet, new TransformTrim("a", TrimType.BOTH, "C", true));
    transformAndValidate(dsGet, new TransformTrim("a", TrimType.LEFT, "C", false));
    transformAndValidate(dsGet, new TransformTrim("a", TrimType.RIGHT, "C", false));
    populated = false;

  }

  @Test
  public void testCaseExistingDataset() throws Exception {
    setSpace();

    DatasetPath datasetPath = new DatasetPath("spacefoo.folderbar.folderbaz.caseexistingdata");
    createDatasetFromParentAndSave(datasetPath, "cp.\"json/stringCaseAndTrim.json\"");
    final DatasetUI dsGet = getDataset(datasetPath);

    doc("uppercase col a");
    transformAndValidate(
        dsGet,
        new TransformConvertCase("a", ConvertCase.UPPER_CASE, "B", false));

    doc("lowercase col B");
    transformAndValidate(
        dsGet,
        new TransformConvertCase("a", ConvertCase.LOWER_CASE, "B", true));

    populated = false;
  }

  @Test
  public void testExtractExistingDataset() throws Exception {
    initData();

    final DatasetUI dsGet = getDataset(new DatasetPath("Prod-Sample.ds1"));

    doc("extract recommendations");
    expectSuccess(
        getBuilder(getAPIv2().path(versionedResourcePath(dsGet) + "/extract"))
            .buildPost(entity(new Selection("address", "address75", 7, 2), JSON)), // => sending
        ExtractCards.class);

    doc("extract preview");
    expectSuccess(
        getBuilder(getAPIv2().path(versionedResourcePath(dsGet) + "/extract_preview"))
            .buildPost(entity(new ExtractPreviewReq(new Selection("address", "address75", 7, 2), DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX)), JSON)), // => sending
        ExtractCard.class);

    {
      doc("extract col address");
      String colName = "res";
      InitialPreviewResponse transformResponse = transformAndValidate(
          dsGet,
          new TransformExtract("address", colName, DatasetsUtil.position(new Offset(3, FROM_THE_START), new Offset(6, FROM_THE_START)), false));
      JobDataFragment data = getData(transformResponse.getPaginationUrl(), 0, 2000);
      for (int i=0; i<data.getReturnedRowCount(); i++) {
        assertEquals("ress", data.extractString(colName, i));
      }
    }
    {
      doc("extract col address 1 char from the end");
      InitialPreviewResponse transformResponse = transformAndValidate(
          dsGet,
          new TransformExtract("address", "s", DatasetsUtil.position(new Offset(1, FROM_THE_END), new Offset(0, FROM_THE_END)), false));
      JobDataFragment data = getData(transformResponse.getPaginationUrl(), 0, 2000);
      for (int i=0; i<data.getReturnedRowCount(); i++) {
        String addressValue = data.extractString("address", i);
        String resValue = data.extractString("s", i);
        assertEquals(addressValue, addressValue.substring(addressValue.length() - 2), resValue);
      }
    }

    doc("extract col address pattern");
    transformAndValidate(
        dsGet,
        new TransformExtract("address", "number", DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX), false));

    doc("extract col same name");
    transformAndValidate(
        dsGet,
        new TransformExtract("address", "address", DatasetsUtil.pattern("\\d+", 0, IndexType.INDEX), true));

    doc("extract col address pattern with non matching pattern");
    transformAndValidate(
        dsGet,
        new TransformExtract("address", "number", DatasetsUtil.pattern("\\d+12", 0, IndexType.INDEX), false));
  }

  @Test
  public void testCreateUntitled() throws Exception {
    initData();
    DatasetUI dataset = createDatasetFromParent("Prod-Sample.ds1").getDataset();

    doc("sort dataset by address");
    transformAndValidate(
        dataset,
        new TransformSort().setSortedColumnName("address"));

    //"{\n\"type\" : \"sort\",\n\"sortedColumnName\" : \"address\"\n}");
  }

  @Test
  public void testCreateUntitledSQL() throws Exception {
    initData();
    DatasetUI dataset =
        createDatasetFromSQL("Select * from \"Prod-Sample\".ds1 limit 1", asList("Prod-Sample")).getDataset();

    doc("sort dataset by address");
    transformAndValidate(
        dataset,
        new TransformSort().setSortedColumnName("address"));
    //"{\n\"type\" : \"sort\",\n\"sortedColumnName\" : \"address\"\n}");
  }

  @Test
  public void testUpdateSQL() throws Exception {
    initData();

    DatasetPath dp = new DatasetPath("Prod-Sample.ds1");
    final DatasetUI dsGet = getDataset(dp);

    doc("update SQL");
    DatasetUI dsTransformed = transformAndValidate(dsGet,
        new TransformUpdateSQL("select * from \"Sales-Sample\".ds3")
            .setSqlContextList(dp.toParentPathList())).getDataset();

    doc("sort dataset by A");
    transformAndValidate(
        dsTransformed,
        new TransformSort().setSortedColumnName("A"));
  }

  @Test
  public void testHistory() throws Exception {
    initData();
    DatasetPath dp = new DatasetPath("Prod-Sample.ds1");
    final DatasetUI dsGet = getDataset(dp);

    doc("update SQL");
    InitialPreviewResponse transformResponse = transformAndValidate(
        dsGet,
        new TransformUpdateSQL("select * from \"Sales-Sample\".ds3").setSqlContextList(dp.toParentPathList()));

    List<HistoryItem> historyItems = transformResponse.getHistory().getItems();
    assertEquals(historyItems.toString(), 2, historyItems.size());
    HistoryItem lastHistoryItem = historyItems.get(1);
    assertEquals(getDatasetVersionPath(transformResponse.getDataset()), lastHistoryItem.getVersionedResourcePath());
    JobState lastItemState = lastHistoryItem.getState();

    // Job is always in completed state.
    assertEquals(JSONUtil.toString(lastHistoryItem), JobState.COMPLETED, lastItemState);
    assertEquals("SQL Edited to: select * from \"Sales-Sample\".ds3", lastHistoryItem.getTransformDescription());
  }

  /**
   * Test info schema queries on virtual/physical datasets.
   */
  @Test
  public void infoSchema() throws Exception {
    final NASConf nas = new NASConf();
    nas.path = new File("src/test/resources").getAbsolutePath();
    SourceUI source = new SourceUI();
    source.setName("testNAS");
    source.setConfig(nas);
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
    final SourceService sourceService = newSourceService();
    sourceService.registerSourceWithRuntime(source);

    // preview a file in NAS source, so that it will be added as a physical dataset
    getPreview(new DatasetPath(asList("testNAS", "datasets", "users.json")));

    // Now create a VDS in home.
    final DatasetPath datasetPath = new DatasetPath(asList("@" + DEFAULT_USERNAME, "vds1"));
    createDatasetFromSQLAndSave(datasetPath, "SELECT * FROM sys.version", asList("cp"));

    InitialPreviewResponse response = createDatasetFromSQL(
        "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, CASE TABLE_TYPE WHEN 'VIEW' THEN 'TABLE' ELSE TABLE_TYPE END as TABLE_TYPE " +
            "FROM INFORMATION_SCHEMA.\"TABLES\" " +
            "WHERE " +
            "      TABLE_CATALOG LIKE 'DREMIO' ESCAPE '\\' AND" +
            "      (TABLE_SCHEMA LIKE 'testNAS.datasets' OR TABLE_SCHEMA LIKE '@" + DEFAULT_USERNAME + "') " +
            "ORDER BY TABLE_SCHEMA",
        asList("cp"));
    assertEquals(2, response.getData().getReturnedRowCount());
    assertEquals("vds1", response.getData().extractString("TABLE_NAME", 0));
    assertEquals("users.json", response.getData().extractString("TABLE_NAME", 1));
  }
}
