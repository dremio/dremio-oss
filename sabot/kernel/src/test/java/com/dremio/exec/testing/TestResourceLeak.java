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
package com.dremio.exec.testing;

import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.PARQUET_SCHEMA_FALLBACK_DISABLED;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.Float8Holder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.QueryTestUtil;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.server.SabotNode;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.test.DremioTest;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/*
 * TODO(DRILL-3170)
 * This test had to be ignored because while the test case tpch01 works, the test
 * fails overall because the final allocator closure again complains about
 * outstanding resources. This could be fixed if we introduced a means to force
 * cleanup of an allocator and all of its descendant resources. But that's a
 * non-trivial exercise in the face of the ability to transfer ownership of
 * slices of a buffer; we can't be sure it is safe to release an
 * UnsafeDirectLittleEndian that an allocator believes it owns if slices of that
 * have been transferred to another allocator.
 */
@Ignore
public class TestResourceLeak extends DremioTest {

  private static DremioClient client;
  private static SabotNode bit;
  private static ClusterCoordinator clusterCoordinator;
  private static SabotConfig config;

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.HTTP_ENABLE, "false");
      put(PARQUET_SCHEMA_FALLBACK_DISABLED, "true");
    }
  };

  @BeforeClass
  public static void openClient() throws Exception {
    config = SabotConfig.create(TEST_CONFIGURATIONS);
    clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();

    bit = new SabotNode(config, clusterCoordinator, DremioTest.CLASSPATH_SCAN_RESULT, true);
    bit.run();
    client = QueryTestUtil.createClient(config, clusterCoordinator, 2, null);
  }

  @Test
  public void tpch01() throws Exception {
    final String query = getFile("memory/tpch01_memory_leak.sql");
    try {
      QueryTestUtil.test(client, "alter session set \"planner.slice_target\" = 10; " + query);
    } catch (UserRemoteException e) {
      if (e.getMessage().contains("Allocator closed with outstanding buffers allocated")) {
        return;
      }
      throw e;
    }
    fail("Expected UserRemoteException indicating memory leak");
  }

  private static String getFile(String resource) throws IOException {
    final URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  @AfterClass
  public static void closeClient() throws Exception {
    try {
      client.close();
      bit.close();
      clusterCoordinator.close();
    } catch (IllegalStateException e) {
      e.printStackTrace();
    }
  }

  @FunctionTemplate(name = "leakResource", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Leak implements SimpleFunction {

    @Param Float8Holder in;
    @Inject ArrowBuf buf;
    @Output Float8Holder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      buf.retain();
      out.value = in.value;
    }
  }
}
