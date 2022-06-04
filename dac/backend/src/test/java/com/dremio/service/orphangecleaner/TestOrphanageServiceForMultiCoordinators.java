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
package com.dremio.service.orphangecleaner;

import java.io.File;
import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.orphanagecleaner.OrphanageCleanerService;

public class TestOrphanageServiceForMultiCoordinators extends BaseTestServer {

  private static OrphanageCleanerService masterOrphanService;
  private static OrphanageCleanerService slaveOrphanService;

  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();


  @BeforeClass
  public static void init() throws Exception {
    // ignore the tests if not multinode.
      System.setProperty("dremio_multinode", "true");
      System.setProperty(DremioConfig.METADATA_PATH_STRING, "file://" + temporaryFolder.getRoot().toString());
      BaseTestServer.init();
      BaseTestServer.getPopulator().populateTestUsers();

      setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getOptionName(), "true");
      setSystemOption(ExecConstants.ENABLE_ICEBERG.getOptionName(), "true");
      setSystemOption(ExecConstants.ORPHANAGE_ENTRY_CLEAN_PERIOD_MINUTES, "1000");

      masterOrphanService = getMasterDremioDaemon().getBindingProvider().lookup(OrphanageCleanerService.class);
      slaveOrphanService = getCurrentDremioDaemon().getBindingProvider().lookup(OrphanageCleanerService.class);

      String metadataRefreshQuery = "REFRESH DATASET cp.nation_ctas.t1.\"0_0_0.parquet\"";
      getQueryProfile(createJobRequest(metadataRefreshQuery));
  }

  @AfterClass
  public static void reset() throws IOException {
    setSystemOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getOptionName(), PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG.getOptionName(), ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ORPHANAGE_ENTRY_CLEAN_PERIOD_MINUTES.getOptionName(), ExecConstants.ORPHANAGE_ENTRY_CLEAN_PERIOD_MINUTES.getDefault().getNumVal().toString());
    FileUtils.deleteDirectory(new File(temporaryFolder.getRoot().toString()));
  }


  private long getOrphanageSize() {
      return StreamSupport.stream(getSabotContext().getOrphanageFactory().get().getAllOrphans().spliterator(), false).count();
  }

  @Test
  public void testOrphanageService() throws Exception {

      String metadataForgetQuery = "ALTER TABLE cp.nation_ctas.t1.\"0_0_0.parquet\" FORGET METADATA";
      getQueryProfile(createJobRequest(metadataForgetQuery));
      slaveOrphanService.cleanup();
      while (getOrphanageSize() != 1) {
        Thread.sleep(1000);
      }
      File[] metadata;
      metadata = temporaryFolder.getRoot()
        .listFiles(p1 -> p1.listFiles(p2 -> p2.getName().equals("metadata")).length > 0);
      Assert.assertTrue(metadata.length > 0);
      masterOrphanService.cleanup();
      while (getOrphanageSize() != 0) {
        Thread.sleep(1000);
      }
      metadata = temporaryFolder.getRoot()
        .listFiles(p1 -> p1.listFiles(p2 -> p2.getName().equals("metadata")).length > 0);
      Assert.assertTrue(metadata.length == 0);

  }

  private static JobRequest createJobRequest(String query) {
      return JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .setDatasetVersion(DatasetVersion.NONE)
        .build();
  }
}
