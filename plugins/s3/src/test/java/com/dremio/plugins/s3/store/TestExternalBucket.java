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
package com.dremio.plugins.s3.store;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.dremio.exec.catalog.conf.Property;
import com.google.common.collect.ImmutableList;

/**
 * Check that external s3 buckets can be listed without an access key.
 */
public class TestExternalBucket {

  @Test
  public void ensureExternalBucketsWork() throws Exception {
    assumeNonMaprProfile();
    try(FileSystem fs = new S3FileSystem()){
      Configuration config = new Configuration();
      S3PluginConfig s3 = new S3PluginConfig();
      s3.externalBuckets = ImmutableList.of("landsat-pds", "commoncrawl");
      for(Property e : s3.getProperties()){
        config.set(e.name, e.value);
      }
      fs.initialize(new URI("dremioS3:///"), config);
      assertEquals(2, fs.listStatus(new Path("/")).length);
    }
  }
}
