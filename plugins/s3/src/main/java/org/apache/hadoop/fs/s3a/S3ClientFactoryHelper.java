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
package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.s3.AmazonS3;

/**
 *
 * S3ClientFactory and DefaultS3ClientFactory in hadoop-aws 2.8.3 are package private
 *
 * Implementing static helper method here to access S3ClientFactory and DefaultS3ClientFactory for creating a S3 client
 *
 * Switch to use original methods directly in hadoop versions 3.0+ (DefaultS3CleintFactory is made public)
 *
 */

public final class S3ClientFactoryHelper {

  private S3ClientFactoryHelper(){}

  public static AmazonS3 createS3ClientHelper(Configuration config, URI name)
    throws IOException {
    DefaultS3ClientFactory clientFactory = new DefaultS3ClientFactory();
    clientFactory.setConf(config);
    return clientFactory.createS3Client(name, "", S3AUtils.createAWSCredentialProviderSet(name, config));
  }
}
