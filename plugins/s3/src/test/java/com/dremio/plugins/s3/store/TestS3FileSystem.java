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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.StsException;

/**
 * Test the S3FileSystem class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(StsClient.class)
public class TestS3FileSystem {
  @Test
  public void testValidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1.amazonaws.com");
    Assert.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-us-gov-west-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.ap-southeast-1.amazonaws.com");
    Assert.assertEquals(Region.AP_SOUTHEAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.ca-central-1.amazonaws.com");
    Assert.assertEquals(Region.CA_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("dremio.s3-control.cn-north-1.amazonaws.com.cn");
    Assert.assertEquals(Region.CN_NORTH_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("accountId.s3-control.dualstack.eu-central-1.amazonaws.com");
    Assert.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.eu-west-1.amazonaws.com");
    Assert.assertEquals(Region.EU_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.dualstack.sa-east-1.amazonaws.com");
    Assert.assertEquals(Region.SA_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-fips.us-gov-west-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.us-gov-east-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_EAST_1, r);
  }

  @Test
  public void testInvalidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("us-west-1");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("abc");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint(null);
    Assert.assertEquals(Region.US_EAST_1, r);
  }

  @Test
  public void testUnknownContainerExists() {
    TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    AmazonS3 mockedS3Client = mock(AmazonS3.class);
    Owner owner = new Owner();
    owner.setId("2350f639447f872b12d9e2298200704aa3b70cea0e127d544748da0351f79118");
    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(true);
    when(mockedS3Client.getS3AccountOwner()).thenReturn(owner);
    AccessControlList acl = getAcl(mockedS3Client);
    when(mockedS3Client.getBucketAcl(any(String.class))).thenReturn(acl);

    fs.setCustomClient(mockedS3Client);
    try {
      assertNotNull(fs.getUnknownContainer("testunknown"));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testUnknownContainerNotExists() {
    TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    AmazonS3 mockedS3Client = mock(AmazonS3.class);
    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(false);
    fs.setCustomClient(mockedS3Client);
    try {
      assertNull(fs.getUnknownContainer("testunknown"));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testUnknownContainerExistsButNoPermissions() {
    TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    AmazonS3 mockedS3Client = mock(AmazonS3.class);
    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(true);
    when(mockedS3Client.getBucketAcl(any(String.class))).thenThrow(new AmazonS3Exception("Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: FF025EBC3B2BF017; S3 Extended Request ID: 9cbmmg2cbPG7+3mXBizXNJ1haZ/0FUhztplqsm/dJPJB32okQRAhRWVWyqakJrKjCNVqzT57IZU=), S3 Extended Request ID: 9cbmmg2cbPG7+3mXBizXNJ1haZ/0FUhztplqsm/dJPJB32okQRAhRWVWyqakJrKjCNVqzT57IZU="));
    fs.setCustomClient(mockedS3Client);
    try {
      assertNull(fs.getUnknownContainer("testunknown"));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testVerifyCredentialsRetry() {
    PowerMockito.mockStatic(StsClient.class);
    StsClient mockedClient = mock(StsClient.class);
    StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);
    when(StsClient.builder()).thenReturn(mockedClientBuilder);

    TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    AtomicInteger retryAttemptNo = new AtomicInteger(1);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).then(invocationOnMock -> {
      if (retryAttemptNo.incrementAndGet() < 10) {
        throw new SdkClientException("Unable to load credentials from service endpoint.");
      }
      return null;
    });

    fs.verifyCredentials(new Configuration());
    assertEquals(10, retryAttemptNo.get());
  }

  @Test(expected = RuntimeException.class)
  public void testVerifyCredentialsNoRetryOnAuthnError() {
    PowerMockito.mockStatic(StsClient.class);
    StsClient mockedClient = mock(StsClient.class);
    StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);
    when(StsClient.builder()).thenReturn(mockedClientBuilder);

    TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    AtomicInteger retryAttemptNo = new AtomicInteger(0);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).then(invocationOnMock -> {
      retryAttemptNo.incrementAndGet();
      throw StsException.builder().message("The security token included in the request is invalid. (Service: Sts, Status Code: 403, Request ID: a7e2e92e-5ebb-4343-87a1-21e4d64edcd4)").build();
    });
    fs.verifyCredentials(new Configuration());
    assertEquals(1, retryAttemptNo.get());
  }

  private AccessControlList getAcl(final AmazonS3 s3Client) {
    ArrayList<Grant> grantCollection = new ArrayList<>();

    // Grant the account owner full control.
    Grant grant1 = new Grant(new CanonicalGrantee(s3Client.getS3AccountOwner().getId()), Permission.FullControl);
    grantCollection.add(grant1);

    // Save grants by replacing all current ACL grants with the two we just created.
    AccessControlList bucketAcl = new AccessControlList();
    bucketAcl.grantAllPermissions(grantCollection.toArray(new Grant[0]));
    return bucketAcl;
  }

  private class TestExtendedS3FileSystem extends S3FileSystem {
    void setCustomClient(AmazonS3 s3) {
      super.s3 = s3;
    }

    @Override
    public AwsCredentialsProvider getAsync2Provider(Configuration conf) {
      AwsCredentialsProvider mockProvider = mock(AwsCredentialsProvider.class);
      return mockProvider;
    }
  }
}
