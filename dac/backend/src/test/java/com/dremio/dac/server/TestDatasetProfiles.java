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

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;
import static com.dremio.exec.proto.UserBitShared.DatasetType.PDS;
import static com.dremio.exec.proto.UserBitShared.DatasetType.VDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;

import javax.ws.rs.client.Entity;

import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.Space;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

/**
 * Testing if we properly record the pds/vds schema/sql in the QueryProfile
 */
public class TestDatasetProfiles extends BaseTestServer {

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  private static String simplePdsPath;
  private static String complexPdsPath;

  private static final BatchSchema simpleSchema = BatchSchema.newBuilder()
    .addField(bigInt(UPDATE_COLUMN))
    .addField(varchar("key"))
    .addField(bigInt("value"))
    .build();
  private static final BatchSchema complexSchema = BatchSchema.newBuilder()
    .addField(bigInt(UPDATE_COLUMN))
    .addField(bigInt("custID"))
    .addField(varchar("cName"))
    .addField(list("cItems", map("$data$", varchar("iName"), dbl("weight_in_pounds"))))
    .addField(map("cProperties", bigInt("a"), dbl("b")))
    .build();

  @BeforeClass
  public static void setup() throws Exception {
    simplePdsPath = addJsonTable("simple_schema.json", "{ \"key\" : \"A\", \"value\" : 0 }");
    complexPdsPath = addJsonTable("complex_schema.json", "{" +
      "\"custID\": 100," +
      "\"cName\": \"customer\"," +
      "\"cItems\": [{ \"iName\": \"laptop\", \"weight_in_pounds\": 2.5 }]," +
      "\"cProperties\": {\"a\": 1000,\"b\": 0.25}" +
      "}");
  }

  @Test
  public void simplePdsSchema() throws Exception {
    // generate some data
    final QueryProfile profile = getQueryProfile(String.format("SELECT * FROM dfs.\"%s\"", simplePdsPath));
    assertEquals(1, profile.getDatasetProfileCount());
    assertTrue(containsPds(profile, simpleSchema));
  }

  @Test
  public void complexPdsSchema() throws Exception {
    final QueryProfile profile = getQueryProfile(String.format("SELECT * FROM dfs.\"%s\"", complexPdsPath));
    assertEquals(1, profile.getDatasetProfileCount());
    assertTrue(containsPds(profile, complexSchema));
  }

  @Test
  public void testVds() throws Exception {
    final String spaceName = "vds_space";
    final String vdsName = "vds";
    try {
      getNamespaceService().getSpace(new NamespaceKey(spaceName));
    } catch (NamespaceNotFoundException e) {
      expectSuccess(getBuilder(getAPIv2().path("space/" + spaceName))
        .buildPut(Entity.json(new Space(null, spaceName, null, null, null, 0, null))), Space.class);
    }

    final DatasetPath vdsPath = new DatasetPath(spaceName + "." + vdsName);
    final String vdsSql = String.format("SELECT * FROM dfs.\"%s\"", complexPdsPath);
    createDatasetFromSQLAndSave(vdsPath, vdsSql, Collections.emptyList());

    final QueryProfile profile = getQueryProfile(String.format("SELECT * FROM %s.%s", spaceName, vdsName));
    assertEquals(2, profile.getDatasetProfileCount());
    assertTrue(containsPds(profile, complexSchema));
    assertTrue(containsVds(profile, vdsSql));
  }

  @Test
  public void testJoinVds() throws Exception {
    final String spaceName = "vds_space2";
    final String vdsName = "join_vds";
    try {
      getNamespaceService().getSpace(new NamespaceKey(spaceName));
    } catch (NamespaceNotFoundException e) {
      expectSuccess(getBuilder(getAPIv2().path("space/" + spaceName))
        .buildPut(Entity.json(new Space(null, spaceName, null, null, null, 0, null))), Space.class);
    }

    final DatasetPath vdsPath = new DatasetPath(spaceName + "." + vdsName);
    final String vdsSql = String.format("SELECT * FROM dfs.\"%s\" t1 LEFT JOIN dfs.\"%s\" t2 ON t1.\"value\" = t2.custID", simplePdsPath, complexPdsPath);
    createDatasetFromSQLAndSave(vdsPath, vdsSql, Collections.emptyList());

    final QueryProfile profile = getQueryProfile(String.format("SELECT * FROM %s.%s", spaceName, vdsName));
    assertEquals(3, profile.getDatasetProfileCount());
    assertTrue(containsPds(profile, simpleSchema));
    assertTrue(containsPds(profile, complexSchema));
    assertTrue(containsVds(profile, vdsSql));
  }

  private boolean containsPds(QueryProfile profile, BatchSchema schema) {
    return profile.getDatasetProfileList().stream().anyMatch(dp ->
      dp.getType() == PDS && schema.equals(BatchSchema.deserialize(dp.getBatchSchema().toByteArray())));
  }

  private boolean containsVds(QueryProfile profile, String vdsSql) {
    return profile.getDatasetProfileList().stream().anyMatch(dp -> dp.getType() == VDS && dp.getSql().equals(vdsSql));
  }

  private static Field bigInt(String name) {
    return new Field(name, true, MinorType.BIGINT.getType(), null);
  }

  private static Field varchar(String name) {
    return new Field(name, true, MinorType.VARCHAR.getType(), null);
  }

  private static Field dbl(String name) {
    return new Field(name, true, MinorType.FLOAT8.getType(), null);
  }

  private static Field map(String name, Field...children) {
    return new Field(name, true, MinorType.STRUCT.getType(), Arrays.asList(children));
  }

  private static Field list(String name, Field inner) {
    return new Field(name, true, MinorType.LIST.getType(),
      Collections.singletonList(inner));
  }

  private static JobsService getJobsService() {
    return p(JobsService.class).get();
  }

  private static NamespaceService getNamespaceService() {
    return p(NamespaceService.class).get();
  }

  private static QueryProfile getQueryProfile(final String query) throws Exception {
    final JobRequest request = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
      .setQueryType(QueryType.UI_INTERNAL_RUN)
      .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.NONE)
      .build();
    return JobsServiceTestUtils.getQueryProfile(l(JobsService.class), request);
  }

  private static String addJsonTable(String tableName, String... jsonData) throws Exception {
    final File file = temp.newFile(tableName);
    final String dataFile = file.getAbsolutePath();
    //TODO write each record in a separate file, so we can cause a union type for example
    try (PrintWriter writer = new PrintWriter(file)) {
      for (String record : jsonData) {
        writer.println(record);
      }
    }
    final DatasetPath path = new DatasetPath(ImmutableList.of("dfs", dataFile));
    final DatasetConfig dataset = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
      .setFullPathList(path.toPathList())
      .setName(path.getLeaf().getName())
      .setCreatedAt(System.currentTimeMillis())
      .setTag(null)
      .setOwner(DEFAULT_USERNAME)
      .setPhysicalDataset(new PhysicalDataset()
        .setFormatSettings(new FileConfig().setType(FileType.JSON))
      );
    final NamespaceService nsService = getNamespaceService();
    nsService.addOrUpdateDataset(path.toNamespaceKey(), dataset);
    return dataFile;
  }
}
