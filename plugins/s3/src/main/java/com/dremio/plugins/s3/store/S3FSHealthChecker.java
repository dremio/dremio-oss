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

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessMode;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.dremio.exec.util.FSHealthChecker;
import com.dremio.io.file.Path;
import com.dremio.plugins.util.CloseableResource;
import com.google.common.annotations.VisibleForTesting;

/**
 * S3 implementation of FSHealthChecker used by storage plugins.
 */
public class S3FSHealthChecker implements FSHealthChecker {

  private static final Logger logger = LoggerFactory.getLogger(S3FSHealthChecker.class);
  private final Configuration fsConf;
  private CloseableResource<AmazonS3> s3Client;

  public S3FSHealthChecker(Configuration fsConf) {
    this.fsConf = fsConf;
  }

  @Override
  public void healthCheck(Path path, Set<AccessMode> mode) throws IOException {
    try (CloseableResource<AmazonS3> s3Ref = getS3V1Client()) {
      s3Ref.incrementRef();
      final AmazonS3 s3 = s3Ref.getResource();
      ListObjectsV2Request request = createRequestForS3HealthCheck(path);
      ListObjectsV2Result response = s3.listObjectsV2(request);
      if (response.getKeyCount() < 1 && !request.getPrefix().isEmpty()) {
        throw new FileNotFoundException("Path not found: " + path);
      }
    } catch (Exception e)
    {
      logger.error("Error while listing S3 objects in " + path, e);
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  protected CloseableResource<AmazonS3> getS3V1Client() throws IOException {
    if (s3Client == null) {
      s3Client = S3FileSystem.createS3V1Client(fsConf);
    }
    return s3Client;
  }

  private static ListObjectsV2Request createRequestForS3HealthCheck(final Path path) {
    String subPath = removeLeadingSlash(path.toString());
    int firstSlash = subPath.indexOf(Path.SEPARATOR);
    String bucketName, prefix;
    if (firstSlash == -1) {
      bucketName = subPath;
      prefix = "";
    } else {
      bucketName = subPath.substring(0, firstSlash);
      prefix = subPath.substring(firstSlash + 1);
    }
    return new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix).withMaxKeys(1);
  }
}
