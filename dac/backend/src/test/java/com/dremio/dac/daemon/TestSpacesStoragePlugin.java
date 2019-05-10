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
package com.dremio.dac.daemon;

import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static com.dremio.dac.server.test.SampleDataPopulator.getFileContentsFromClassPath;
import static com.dremio.service.namespace.TestNamespaceService.addFolder;
import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Test spaces storage plugin.
 */
public class TestSpacesStoragePlugin extends BaseTestServer {

  public static void setup(DACDaemon dremioDaemon) throws Exception {
    getPopulator().populateTestUsers();
    final File root = getPopulator().getPath().toFile();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(root, "testSpaceA.json")))) {
      for (int i = 0; i < 1000; ++i) {
        writer.write(String.format("{ \"A\" : %d , \"B\": %d }", i, i));
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(root, "testSpaceB.json")))) {
      for (int i = 500; i < 1000; ++i) {
        writer.write(String.format("{ \"C\" : %d , \"D\": %d }", i, i));
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(root, "testSpaceC.json")))) {
      for (int i = 750; i < 1000; ++i) {
        writer.write(String.format("{ \"E\" : %d , \"F\": %d }", i, i));
      }
    }

    final NamespaceService namespaceService = newNamespaceService();
    clearAllDataExceptUser();
    getPopulator().populateSources();
    SpaceConfig config = new SpaceConfig();
    config.setName("testA");
    namespaceService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("testB");
    namespaceService.addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);
    final HomeConfig home1 = new HomeConfig();
    home1.setOwner(DEFAULT_USER_NAME);
    namespaceService.addOrUpdateHome(new HomePath(HomeName.getUserHomePath(home1.getOwner())).toNamespaceKey(), home1);


    getPopulator().putDS("testA", "dsA1", new FromTable("LocalFS1.\"testSpaceA.json\"").wrap());
    getPopulator().putDS("testB", "dsB1", new FromTable("LocalFS1.\"testSpaceB.json\"").wrap());
    getPopulator().putDS("testA", "dsA2", new FromTable("LocalFS1.\"testSpaceC.json\"").wrap());
    getPopulator().putDS("testA", "dsA3", new FromSQL(getFileContentsFromClassPath("queries/tpch/03.sql")
        .replaceAll("\\-\\-.*", "")
        .replace('`', '"')
        .replace(';', ' ')
        ).wrap());

    addFolder(namespaceService, "testA.F1");
    addFolder(namespaceService, "testA.F1.F2");
    addFolder(namespaceService, "testA.F1.F2.F3");
    addFolder(namespaceService, "testA.F1.F2.F3.F4");

    addFolder(namespaceService, "@"+DEFAULT_USER_NAME+".F1");
    addFolder(namespaceService, "@"+DEFAULT_USER_NAME+".F1.F2");
    addFolder(namespaceService, "@"+DEFAULT_USER_NAME+".F1.F2.F3");
    addFolder(namespaceService, "@"+DEFAULT_USER_NAME+".F1.F2.F3.F4");

    List<FolderName> folderPath = new ArrayList<>();
    folderPath.add(new FolderName("F1"));
    getPopulator().putDS("testA", folderPath, "dsA1", new FromTable("LocalFS1.\"testSpaceA.json\"").wrap());
    getPopulator().putDS("@"+DEFAULT_USER_NAME, folderPath, "dsA1", new FromTable("LocalFS1.\"testSpaceA.json\"").wrap());
    folderPath.add(new FolderName("F2"));
    getPopulator().putDS("testA", folderPath, "dsB1", new FromTable("LocalFS1.\"testSpaceB.json\"").wrap());
    getPopulator().putDS("@"+DEFAULT_USER_NAME, folderPath, "dsB1", new FromTable("LocalFS1.\"testSpaceB.json\"").wrap());
    folderPath.add(new FolderName("F3"));
    getPopulator().putDS("testA", folderPath, "dsA2", new FromTable("LocalFS1.\"testSpaceC.json\"").wrap());
    getPopulator().putDS("@"+DEFAULT_USER_NAME, folderPath, "dsA2", new FromTable("LocalFS1.\"testSpaceC.json\"").wrap());
    folderPath.add(new FolderName("F4"));
    getPopulator().putDS("testA", folderPath, "dsA3", new FromSQL(getFileContentsFromClassPath("queries/tpch/03.sql")
        .replaceAll("\\-\\-.*", "")
        .replace('`', '"')
        .replace(';', ' ')
        ).wrap());
    getPopulator().putDS("@"+DEFAULT_USER_NAME, folderPath, "dsA3", new FromSQL(getFileContentsFromClassPath("queries/tpch/03.sql")
        .replaceAll("\\-\\-.*", "")
        .replace('`', '"')
        .replace(';', ' ')
        ).wrap());
  }

  public static void cleanup(DACDaemon dremioDaemon) throws Exception {
    final NamespaceService namespaceService = newNamespaceService();
    namespaceService.deleteSpace(new SpacePath("testA").toNamespaceKey(), namespaceService.getSpace(new SpacePath("testA").toNamespaceKey()).getTag());
    namespaceService.deleteSpace(new SpacePath("testB").toNamespaceKey(), namespaceService.getSpace(new SpacePath("testB").toNamespaceKey()).getTag());
  }

  private JobData runExternalQuery(String sql) {
    return JobUI.getJobData(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(sql, Collections.singletonList("@" + DEFAULT_USER_NAME), DEFAULT_USER_NAME))
        .build(), NoOpJobStatusListener.INSTANCE));
  }

  private JobsService jobsService;

  @Before
  public void setUpQueryExecutor() {
    jobsService = l(JobsService.class);
  }

  @Test
  public void testSpacesPlugin() throws Exception {
    setup(getCurrentDremioDaemon());
    // update storage plugin
    JobDataFragment results;

    results = runExternalQuery("select * from testA.dsA1").truncate(1000);
    //System.out.println(JSONUtil.toString(results));
    assertEquals(1000, results.getReturnedRowCount());

    results = runExternalQuery("select * from testB.dsB1").truncate(500);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(500, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.dsA2").truncate(250);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(250, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.dsA1 t1 where t1.A >= 400").truncate(600);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(600, results.getReturnedRowCount());

    results = runExternalQuery(
            "select * from testA.dsA1 t1 inner join testB.dsB1 t2 on t1.A = t2.C inner join testA.dsA2 t3 on t2.C = t3.E where t3.F >= 900").truncate(100);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(100, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.dsA3").truncate(10);
    System.out.println(JSONUtil.toString(results));
    assertEquals(10, results.getReturnedRowCount());

    // folder/subschemas
    results = runExternalQuery("select * from testA.F1.dsA1").truncate(1000);
    //System.out.println(JSONUtil.toString(results));
    assertEquals(1000, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.F1.F2.dsB1").truncate(500);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(500, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.F1.F2.F3.dsA2").truncate(250);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(250, results.getReturnedRowCount());

    results = runExternalQuery("select * from testA.F1.F2.F3.F4.dsA3").truncate(10);
    //System.out.println(JSONUtil.toString(results));
    assertEquals(10, results.getReturnedRowCount());

    results = runExternalQuery("select * from \"@"+DEFAULT_USER_NAME+"\".F1.dsA1").truncate(1000);
    //System.out.println(JSONUtil.toString(results));
    assertEquals(1000, results.getReturnedRowCount());

    results = runExternalQuery("select * from \"@"+DEFAULT_USER_NAME+"\".F1.F2.dsB1").truncate(500);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(500, results.getReturnedRowCount());

    results = runExternalQuery("select * from \"@"+DEFAULT_USER_NAME+"\".F1.F2.F3.dsA2").truncate(250);
    //System.out.println(PrettyJSON.toString(results));
    assertEquals(250, results.getReturnedRowCount());

    results = runExternalQuery("select * from \"@"+DEFAULT_USER_NAME+"\".F1.F2.F3.F4.dsA3").truncate(10);
//    System.out.println(JSONUtil.toString(results));
    assertEquals(10, results.getReturnedRowCount());


    cleanup(getCurrentDremioDaemon());
  }
}
