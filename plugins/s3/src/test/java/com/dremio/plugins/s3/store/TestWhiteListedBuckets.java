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

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.regions.Region;


/**
 * Check that Dremio Whitelisted buckets work with S3 compatible systems via S3Mock.
 */
public class TestWhiteListedBuckets extends BaseTestQuery {

  private static S3Mock s3Mock;
  private static int port;

  @BeforeClass
  public static void setup() {
    Preconditions.checkState(s3Mock == null);
    s3Mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();
    port = s3Mock.start().localAddress().getPort();

    EndpointConfiguration endpoint = new EndpointConfiguration(String.format("http://localhost:%d", port), Region.US_EAST_1.toString());
    AmazonS3 client = AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build();

    client.createBucket("bucket-a");
    client.createBucket("bucket-b");
    client.createBucket("bucket-c");
  }

  @AfterClass
  public static void teardown() {
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  @Test
  public void s3TestAllBucketsAreListedWhenWhiteListedBucketsAreEmpty() throws Exception {
    assumeNonMaprProfile();
    try (FileSystem fs = new S3FileSystem() {
      @Override
      protected void verifyCredentials(Configuration config) {
        // Override verifyCredentials method to pass with dummy credentials.
      }
    }) {
      Configuration config = setupTestConfiguration(Lists.newArrayList());
      fs.initialize(new URI("dremioS3:///"), config);
      FileStatus[] buckets = fs.listStatus(new Path("/"));
      assertEquals(3, buckets.length);
      assertEquals("bucket-a", buckets[0].getPath().toString());
      assertEquals("bucket-b", buckets[1].getPath().toString());
      assertEquals("bucket-c", buckets[2].getPath().toString());
    }
  }

  @Test
  public void s3TestOnlyWhiteListedBucketsAreListedWhenWhiteListedBucketsAreSpecified() throws Exception {
    assumeNonMaprProfile();
    try (FileSystem fs = new S3FileSystem() {
      @Override
      protected void verifyCredentials(Configuration config) {
        // Override verifyCredentials method to pass with dummy credentials.
      }
    }) {
      Configuration config = setupTestConfiguration(Lists.newArrayList("bucket-a", "bucket-c"));
      fs.initialize(new URI("dremioS3:///"), config);
      FileStatus[] buckets = fs.listStatus(new Path("/"));
      assertEquals(2, buckets.length);
      assertEquals("bucket-a", buckets[0].getPath().toString());
      assertEquals("bucket-c", buckets[1].getPath().toString());
    }
  }

  @Test(expected = RuntimeException.class)
  public void s3TestInvalidCredentialsThrowRuntimeExceptionWhenWhiteListedBucketsAreEmpty() throws Exception {
    assumeNonMaprProfile();
    try (FileSystem fs = new S3FileSystem()) {
      Configuration config = setupTestConfiguration(Lists.newArrayList());
      fs.initialize(new URI("dremioS3:///"), config);
    }
  }

  @Test(expected = RuntimeException.class)
  public void s3TestInvalidCredentialsThrowRuntimeExceptionWhenWhiteListedBucketsAreSpecified() throws Exception {
    assumeNonMaprProfile();
    try (FileSystem fs = new S3FileSystem()) {
      Configuration config = setupTestConfiguration(Lists.newArrayList("bucket-a", "bucket-c"));
      fs.initialize(new URI("dremioS3:///"), config);
    }
  }

  private Configuration setupTestConfiguration(List<String> whiteListedBuckets) throws RuntimeException {
    Configuration config = new Configuration();
    S3PluginConfig s3 = new S3PluginConfig();
    s3.credentialType = AWSAuthenticationType.ACCESS_KEY;
    s3.accessKey = "foo";
    s3.accessSecret = "bar";
    s3.secure = false;
    s3.propertyList = Lists.newArrayList(
      new Property("fs.s3a.endpoint", "localhost:" + port),
      new Property("fs.s3a.path.style.access", "true"));
    if (!whiteListedBuckets.isEmpty()) {
      s3.whitelistedBuckets = whiteListedBuckets;
    }

    SabotContext context = getSabotContext();
    S3StoragePlugin plugin = s3.newPlugin(context, "test-plugin", null);
    for (Property e : plugin.getProperties()) {
      config.set(e.name, e.value);
    }
    return config;
  }
}
