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
package com.dremio.dac.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;

import javax.ws.rs.client.Entity;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.beust.jcommander.JCommander;
import com.dremio.common.perf.Timer;
import com.dremio.common.utils.ProtobufUtils;
import com.dremio.dac.cmd.ExportProfiles.ExportProfilesOptions;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.resource.ExportProfilesParams;
import com.dremio.dac.resource.ExportProfilesStats;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.test.DremioTest;

/**
 * Test export-profiles command.
 */
public class TestExportProfiles extends BaseTestServer {

  private static UserBitShared.QueryProfile queryProfile = null;

  private static DACConfig dacConfig =  DACConfig
    .newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
    .autoPort(true)
    .allowTestApis(true)
    .serveUI(false)
    .inMemoryStorage(false)
    .clusterMode(DACDaemon.ClusterMode.LOCAL);

  private static FileSystem fs;

  @BeforeClass
  public static void init() throws Exception {
    fs = HadoopFileSystem.getLocal(new Configuration());
    Assume.assumeFalse(BaseTestServer.isMultinode());
    try (Timer.TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      dacConfig = dacConfig.writePath(folder1.newFolder().getAbsolutePath());
      startDaemon();
      queryProfile = runDummyJob();
    }
  }

  public static void startDaemon() throws Exception {
    setCurrentDremioDaemon(DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT));
    setMasterDremioDaemon(null);
    getCurrentDremioDaemon().init();
    initClient();
    setBinder(createBinder(getCurrentDremioDaemon().getBindingProvider()));
  }

  public static byte[] readAll(InputStream is) throws IOException {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    while (is.available() > 0) {
      os.write(is.read());
    }
    return os.toByteArray();
  }

  private static UserBitShared.QueryProfile runDummyJob() throws Exception {
    // Run a job to generate a profile to export
    final JobsService jobsService = l(JobsService.class);
    final JobRequest jobRequest = JobRequest.newBuilder().setSqlQuery(new SqlQuery("select * from sys.reflections", DEFAULT_USERNAME)).build();
    final JobId jobId = JobsServiceTestUtils.submitJobAndWaitUntilCompletion(jobsService, jobRequest);

    // Get a profile from the job
    final QueryProfileRequest request = QueryProfileRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).setAttempt(0).build();

    return jobsService.getProfile(request);
  }

  private void verifyResult(String path, UserBitShared.QueryProfile queryProfile, String command) throws Exception {

    final Path profilePath = fs.list(Path.of(path),PathFilters.ALL_FILES).iterator().next().getPath();
    assertTrue(String.format("Failed to find profile from expected location (command: export-profiles %s)", command), fs.list(profilePath, PathFilters.endsWith(".json")).iterator().hasNext());

    final InputStream is = fs.open(fs.list(profilePath, PathFilters.endsWith(".json")).iterator().next().getPath());
    byte[] b = readAll(is);
    is.close();

    final UserBitShared.QueryProfile profile = ProtobufUtils.fromJSONString(UserBitShared.QueryProfile.class, new String(b));
    assertEquals(String.format("Failed to verify returned profile (command: export-profiles %s)", command), profile.toString(),queryProfile.toString());
  }

  private ExportProfilesOptions getExportOptions(String[] args) {
    final ExportProfilesOptions options = new ExportProfilesOptions();
    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.parse(args);
    ExportProfiles.setTime(options);
    ExportProfiles.setPath(options);
    return options;
  }

  @Test
  public void testOnline() throws Exception {
    final String tmpPath = folder0.newFolder("testOnline").getAbsolutePath();
    expectSuccess(getBuilder(getAPIv2().path("export-profiles"))
      .buildPost(Entity.entity(new ExportProfilesParams(tmpPath, ExportProfilesParams.WriteFileMode.FAIL_IF_EXISTS, null, null, ExportProfilesParams.ExportFormatType.JSON, 1), JSON)), ExportProfilesStats.class);
    verifyResult(tmpPath, queryProfile, "online");
  }

  @Test
  public void testLocal() throws Exception {
    final String tmpPath = folder0.newFolder("testLocal").getAbsolutePath();
    final String[] args = {"-l", "--output", tmpPath, "--format", "JSON", "--write-mode", "FAIL_IF_EXISTS"};
    final ExportProfilesOptions options = getExportOptions(args);
    String vmid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    DremioAttach.main(vmid, new String[] {"export-profiles", ExportProfiles.getAPIExportParams(options).toParamString()});
    verifyResult(tmpPath, queryProfile, String.join(" ", args));
  }

  @Test
  public void testOffline() throws Exception {
    final String tmpPath = folder0.newFolder("testOffline").getAbsolutePath();
    final LegacyKVStoreProvider localKVStoreProvider = l(LegacyKVStoreProvider.class);

    final LegacyKVStoreProvider spy = spy(localKVStoreProvider);
    Mockito.doNothing().when(spy).close();

    final String[] args = {"-o", "--output", tmpPath, "--format", "JSON", "--write-mode", "FAIL_IF_EXISTS"};
    final ExportProfiles.ExportProfilesOptions options = getExportOptions(args);
    ExportProfiles.exportOffline(options, spy);
    verifyResult(tmpPath, queryProfile, String.join(" ", args));
  }
}
