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
package com.dremio.plugins.s3.store;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.findify.s3mock.S3Mock;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/** Check that Dremio works with S3 compatible systems via S3Mock. */
public class TestS3Compat extends BaseTestQuery {

  private S3Mock api;
  private int port;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(120, TimeUnit.SECONDS);

  @Before
  public void setup() {
    Preconditions.checkState(api == null);
    api =
        new S3Mock.Builder()
            .withPort(0)
            .withFileBackend(TestTools.getWorkingPath() + "/src/test/resources/s3compat")
            .build();
    port = api.start().localAddress().getPort();
  }

  @After
  public void teardown() {
    if (api != null) {
      api.shutdown();
      api = null;
    }
  }

  @Test
  public void s3Compat() throws Exception {
    assumeNonMaprProfile();
    try (FileSystem fs = new S3FileSystem()) {
      Configuration config = new Configuration();
      S3PluginConfig s3 = new S3PluginConfig();

      s3.compatibilityMode = true;
      s3.credentialType = AWSAuthenticationType.ACCESS_KEY;
      s3.accessKey = "foo";
      s3.accessSecret = () -> "bar";
      s3.secure = false;
      s3.propertyList =
          ImmutableList.of(
              new Property("fs.s3a.endpoint", "localhost:" + port),
              new Property("fs.s3a.path.style.access", "true"));

      SabotContext context = getSabotContext();
      S3StoragePlugin plugin = s3.newPlugin(context, "test-plugin", null);
      for (Property e : plugin.getProperties()) {
        config.set(e.name, e.value);
      }
      fs.initialize(new URI("dremioS3:///"), config);
      FileStatus[] buckets = fs.listStatus(new Path("/"));
      assertEquals(1, buckets.length);
      assertEquals("bucket1", buckets[0].getPath().toString());
      assertEquals(1, fs.listStatus(new Path("/bucket1/")).length);
    }
  }
}
