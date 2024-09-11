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
package com.dremio.exec.physical.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PhysicalPlanReaderTestFactory;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.net.URI;
import org.junit.Test;

public class TestParsePhysicalPlan extends ExecTest {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestParsePhysicalPlan.class);

  @Test
  public void parseSimplePlan() throws Exception {
    PhysicalPlanReader reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader();
    LogicalPlanPersistence lpp = reader.getLpPersistance();
    ObjectWriter writer = lpp.getMapper().writer();
    PhysicalPlan plan = reader.readPhysicalPlan(readResourceAsString("/physical_test1.json"));
    String unparse = plan.unparse(writer);
  }

  @Test
  public void ensureFilesystemIsType() throws Exception {
    LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    ObjectMapper mapper = lpp.getMapper();
    String val =
        mapper.writeValueAsString(
            new ExampleHolder(new FileSystemConfig(new URI("file:///"), SchemaMutability.ALL)));
    assertTrue(
        String.format(
            "Expected config to contain %s but did not. Actual value %s.",
            FileSystemConfig.NAME, val),
        val.contains(FileSystemConfig.NAME));
  }

  public static class ExampleHolder {
    public StoragePluginConfig config;

    @JsonCreator
    public ExampleHolder(@JsonProperty("config") StoragePluginConfig config) {
      super();
      this.config = config;
    }
  }

  @Test
  public void delegateReadDX7316() throws Exception {
    LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    ObjectMapper mapper = lpp.getMapper();
    ExampleHolder holder =
        mapper.readValue("{\"config\": {\"type\": \"__delegate\"}}}", ExampleHolder.class);
    assertEquals(FileSystemConfig.class, holder.config.getClass());
  }

  @Test
  public void pdfsReadDX7316() throws Exception {
    LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    ObjectMapper mapper = lpp.getMapper();
    ExampleHolder holder =
        mapper.readValue("{\"config\": {\"type\": \"pdfs\"}}}", ExampleHolder.class);
    assertEquals(FileSystemConfig.class, holder.config.getClass());
  }
}
