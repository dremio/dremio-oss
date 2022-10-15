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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.dremio.exec.util.FSHealthChecker;
import com.dremio.io.file.Path;
import com.dremio.plugins.util.CloseableResource;
import com.google.common.collect.ImmutableSet;

/**
 * Test the S3FSHealthChecker class.
 */
public class TestS3FSHealthChecker {

  @Test
  public void testGoodHealthCheck() {
    TestExtendedS3FSHealthChecker fs = new TestExtendedS3FSHealthChecker(new Configuration());
    AmazonS3 mockedS3Client = mock(AmazonS3.class);
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(1);
    when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    fs.setCustomClient(mockedS3Client);

    try {
      Path p = Path.of("/bucket/prefix");
      fs.healthCheck(p, ImmutableSet.of());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test (expected = IOException.class)
  public void testBadHealthCheck() throws IOException {
    TestExtendedS3FSHealthChecker fs = new TestExtendedS3FSHealthChecker(new Configuration());
    AmazonS3 mockedS3Client = mock(AmazonS3.class);
    ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(0);
    when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    fs.setCustomClient(mockedS3Client);

    Path p = Path.of("/bucket/prefix");
    fs.healthCheck(p, ImmutableSet.of());
  }

  @Test
  public void testS3FSHealthCheckerClassInstance() {
    String scheme = new String("dremioS3:///");
    Optional<FSHealthChecker> healthCheckerClass = FSHealthChecker.getInstance(Path.of("/bucket/test"), scheme, new Configuration());
    assertTrue(healthCheckerClass.isPresent());
    assertTrue(healthCheckerClass.get() instanceof S3FSHealthChecker);
  }

  private class TestExtendedS3FSHealthChecker extends S3FSHealthChecker {
    private AmazonS3 s3;

    public TestExtendedS3FSHealthChecker(Configuration fsConf) {
      super(fsConf);
    }

    void setCustomClient(AmazonS3 s3) {
      this.s3 = s3;
    }

    @Override
    protected CloseableResource<AmazonS3> getS3V1Client() throws IOException {
      return new CloseableResource(s3, s3 -> {});
    }
  }
}
