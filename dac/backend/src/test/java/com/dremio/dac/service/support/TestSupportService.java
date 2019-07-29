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
package com.dremio.dac.service.support;

import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.support.SupportResponse;
import com.dremio.dac.support.SupportService;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test the support service
 */
public class TestSupportService extends BaseTestServer {

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  @ClassRule
  public static final TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void init() throws Exception {
    // set the log path so we can read logs and confirm that is working.
    final File jsonFolder = temp.newFolder("json");
    jsonFolder.mkdir();
    Files.copy(new File(Resources.getResource("support/server.json").getPath()), new File(jsonFolder, "server.json"));
    System.setProperty(SupportService.DREMIO_LOG_PATH_PROPERTY, temp.getRoot().toString());

    // now start server.
    BaseTestServer.init();
  }

  @Test
  public void supportDownload() throws Exception {
    InitialPreviewResponse preview = createDatasetFromParent("cp.\"tpch/supplier.parquet\"");
    String url = preview.getPaginationUrl().replace("/job/", "/support/").replace("/data", "" + "/download");

    Response response = getBuilder(url).post(null);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void successSupport() throws Exception {
    InitialPreviewResponse preview = createDatasetFromParent("cp.\"tpch/supplier.parquet\"");
    String url = preview.getPaginationUrl().replace("/job/", "/support/").replace("/data", "");

    SupportResponse response = expectSuccess(getBuilder(url).buildPost(null), SupportResponse.class);

    // We really don't want to actually upload the S3 in tests, so it fails after creating the bundle.
    // Keep calm, carry on.
    assertFalse(response.isSuccess());
    assertTrue(response.getUrl().startsWith("Unable to upload diagnostics in debug, available locally at: "));
  }

  @Test
  @Ignore("DX-4093")
  public void failSupport() throws Exception {
    Assume.assumeTrue(!BaseTestServer.isMultinode());

    String badFileName = "fake_file_one";
    final Invocation invocation = getBuilder(
        getAPIv2()
            .path("datasets/new_untitled_sql")
            .queryParam("newVersion", newVersion())
    ).buildPost(Entity.entity(new CreateFromSQL("select * from " + badFileName, null), MediaType.APPLICATION_JSON_TYPE));
    expectError(FamilyExpectation.CLIENT_ERROR, invocation, Object.class);

    List<Job> jobs = ImmutableList.copyOf(l(JobsService.class).getAllJobs("*=contains=" + badFileName, null, null, 0, 1, null));
    assertEquals(1, jobs.size());
    Job job = jobs.get(0);
    String url = "/support/" + job.getJobId().getId();
    SupportResponse response = expectSuccess(getBuilder(url).buildPost(null), SupportResponse.class);
    assertTrue(response.getUrl().contains("Unable to upload diagnostics"));
    assertTrue("Failure including logs.", response.isIncludesLogs());
  }

}
