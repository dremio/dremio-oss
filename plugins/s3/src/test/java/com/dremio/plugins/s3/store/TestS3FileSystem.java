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

import static com.dremio.plugins.Constants.DREMIO_ENABLE_BUCKET_DISCOVERY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Permission;
import com.dremio.exec.ExecConstants;
import com.dremio.plugins.util.CloseableRef;
import com.dremio.plugins.util.CloseableResource;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.plugins.util.ContainerNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.StsException;

/** Test the S3FileSystem class. */
public class TestS3FileSystem {
  private final String enableS3V2ClientFeatureFlagName =
      ExecConstants.ENABLE_S3_V2_CLIENT.getOptionName();

  @Test
  public void testGetEndpointAddsSchemeOnlyWhenMissing() {
    final Configuration conf = new Configuration(false);
    Assertions.assertFalse(S3FileSystem.getEndpoint(conf).isPresent());

    conf.set(Constants.ENDPOINT, "minio.local:9000");
    Assertions.assertEquals("https://minio.local:9000", S3FileSystem.getEndpoint(conf).get());
    conf.set(Constants.SECURE_CONNECTIONS, "false");
    Assertions.assertEquals("http://minio.local:9000", S3FileSystem.getEndpoint(conf).get());
  }

  @Test
  public void testGetEndpointKeepsScheme() {
    final Configuration conf = new Configuration(false);
    conf.set(Constants.ENDPOINT, " http://minio.local:9000 ");
    // The scheme of the endpoint wins over fs.s3a.connection.ssl.enabled (default true).
    Assertions.assertEquals("http://minio.local:9000", S3FileSystem.getEndpoint(conf).get());

    conf.set(Constants.ENDPOINT, "https://minio.local:9443");
    conf.set(Constants.SECURE_CONNECTIONS, "false");
    Assertions.assertEquals("https://minio.local:9443", S3FileSystem.getEndpoint(conf).get());
  }

  @Test
  public void testValidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1.amazonaws.com");
    Assertions.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-us-gov-west-1.amazonaws.com");
    Assertions.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.ap-southeast-1.amazonaws.com");
    Assertions.assertEquals(Region.AP_SOUTHEAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.ca-central-1.amazonaws.com");
    Assertions.assertEquals(Region.CA_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("dremio.s3-control.cn-north-1.amazonaws.com.cn");
    Assertions.assertEquals(Region.CN_NORTH_1, r);

    r =
        S3FileSystem.getAwsRegionFromEndpoint(
            "accountId.s3-control.dualstack.eu-central-1.amazonaws.com");
    Assertions.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.eu-west-1.amazonaws.com");
    Assertions.assertEquals(Region.EU_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.dualstack.sa-east-1.amazonaws.com");
    Assertions.assertEquals(Region.SA_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-fips.us-gov-west-1.amazonaws.com");
    Assertions.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.us-gov-east-1.amazonaws.com");
    Assertions.assertEquals(Region.US_GOV_EAST_1, r);
  }

  @Test
  public void testInvalidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("us-west-1");
    Assertions.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1");
    Assertions.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("abc");
    Assertions.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("");
    Assertions.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint(null);
    Assertions.assertEquals(Region.US_EAST_1, r);
  }

  @Test
  public void testUnknownContainerExistsV1() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(enableS3V2ClientFeatureFlagName, "false");

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(conf);

    final AmazonS3 mockedS3Client = mock(AmazonS3.class);

    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(true);
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName("testunknown");
    when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    fs.setCustomV1Client(new CloseableResource(mockedS3Client, s3 -> {}));
    fs.setInstantNow(Instant.now());
    Assertions.assertNotNull(fs.getUnknownContainer("testunknown"));
  }

  @Test
  public void testUnknownContainerExists() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final S3Client mockedS3Client = mock(S3Client.class);

    when(mockedS3Client.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().build());

    fs.setCustomV2Client(new CloseableRef<>(mockedS3Client));
    fs.setInstantNow(Instant.now());
    Assertions.assertNotNull(fs.getUnknownContainer("testunknown"));
  }

  @Test
  public void testUnderlyingFSClosesOnFSCloseV1() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(enableS3V2ClientFeatureFlagName, "false");

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(conf);
    final String bucketName = "someContainer";

    // Set up S3 Mock
    final AmazonS3 s3Mock = mock(AmazonS3.class);
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName(bucketName);

    when(s3Mock.doesBucketExistV2(any(String.class))).thenReturn(true);
    when(s3Mock.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    when(s3Mock.listBuckets())
        .thenReturn(List.of(new com.amazonaws.services.s3.model.Bucket(bucketName)));

    fs.setCustomV1Client(new CloseableResource(s3Mock, s3 -> {}));
    fs.setInstantNow(Instant.now());

    // Set up Verify Mocks to simulate a success
    final StsClient mockedClient = mock(StsClient.class);
    final StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
        .thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(null);

    // Mocking static calls for Hadoop's FileSystem in order to return our custom S3AFileSystem,
    // which can be used to keep track of closed state without relying on static properties.
    try (MockedStatic<FileSystem> mockedHadoopFS = mockStatic(FileSystem.class);
        MockedStatic<StsClient> mockedStsClient = mockStatic(StsClient.class)) {
      final TestExtendedS3AFileSystem fakeHadoopFs = new TestExtendedS3AFileSystem(bucketName);
      mockedHadoopFS.when(() -> FileSystem.get(any(), any())).thenReturn(fakeHadoopFs);
      mockedStsClient.when(StsClient::builder).thenReturn(mockedClientBuilder);

      // Initialize the fs and call getFileStatus to ensure fakeHadoopFs gets cached.
      fs.initialize(new URI("s3a://" + bucketName), conf);
      fs.getFileStatus(new Path("/" + bucketName));

      // Assert underlying cached fakeHadoopFs is closed when fs is closed.
      Assertions.assertFalse(fakeHadoopFs.wasFSClosed());
      fs.close();
      Assertions.assertTrue(fakeHadoopFs.wasFSClosed());
    }
  }

  @Test
  public void testUnderlyingFSClosesOnFSClose() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    final String bucketName = "someContainer";

    final S3Client s3Mock = mock(S3Client.class);

    when(s3Mock.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion(Region.US_EAST_1.toString()).build());
    when(s3Mock.listBuckets())
        .thenReturn(
            ListBucketsResponse.builder()
                .buckets(Bucket.builder().name(bucketName).build())
                .build());

    fs.setCustomV2Client(new CloseableRef(s3Mock));
    fs.setInstantNow(Instant.now());

    // Set up Verify Mocks to simulate a success
    final StsClient mockedClient = mock(StsClient.class);
    final StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
        .thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(null);

    // Mocking static calls for Hadoop's FileSystem in order to return our custom S3AFileSystem,
    // which can be used to keep track of closed state without relying on static properties.
    try (MockedStatic<FileSystem> mockedHadoopFS = mockStatic(FileSystem.class);
        MockedStatic<StsClient> mockedStsClient = mockStatic(StsClient.class)) {
      final TestExtendedS3AFileSystem fakeHadoopFs = new TestExtendedS3AFileSystem(bucketName);
      mockedHadoopFS.when(() -> FileSystem.get(any(), any())).thenReturn(fakeHadoopFs);
      mockedStsClient.when(StsClient::builder).thenReturn(mockedClientBuilder);

      // Initialize the fs and call getFileStatus to ensure fakeHadoopFs gets cached.
      fs.initialize(new URI("s3a://" + bucketName), new Configuration());
      fs.getFileStatus(new Path("/" + bucketName));

      // Assert underlying cached fakeHadoopFs is closed when fs is closed.
      Assertions.assertFalse(fakeHadoopFs.wasFSClosed());
      fs.close();
      Assertions.assertTrue(fakeHadoopFs.wasFSClosed());
    }
  }

  @Test
  public void testUnknownContainerNotFoundV1() {
    final Configuration conf = new Configuration();
    conf.set(enableS3V2ClientFeatureFlagName, "false");

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(conf);

    final AmazonS3 mockedS3Client = mock(AmazonS3.class);

    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(false);
    fs.setCustomV1Client(new CloseableResource(mockedS3Client, s3 -> {}));

    ContainerNotFoundException exception =
        Assertions.assertThrows(
            ContainerNotFoundException.class,
            () -> {
              fs.getUnknownContainer("testunknown");
            });
    Assertions.assertEquals(
        "Error while trying to find bucket: testunknown", exception.getMessage());
  }

  @Test
  public void testUnknownContainerNotFound() {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final S3Client mockedS3Client = mock(S3Client.class);

    when(mockedS3Client.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(NoSuchBucketException.builder().build());

    fs.setCustomV2Client(new CloseableRef<>(mockedS3Client));
    fs.setInstantNow(Instant.now());

    ContainerNotFoundException exception =
        Assertions.assertThrows(
            ContainerNotFoundException.class,
            () -> {
              fs.getUnknownContainer("testunknown");
            });
    Assertions.assertEquals(
        "Cannot find bucket: testunknown does not exist", exception.getMessage());
  }

  @Test
  public void testUnknownContainerExistsButNoPermissionsV1() {
    final Configuration conf = new Configuration();
    conf.set(enableS3V2ClientFeatureFlagName, "false");

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(conf);

    final AmazonS3 mockedS3Client = mock(AmazonS3.class);

    when(mockedS3Client.doesBucketExistV2(any(String.class))).thenReturn(true);
    AmazonServiceException toThrow =
        new AmazonS3Exception(
            "Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: FF025EBC3B2BF017; S3 Extended Request ID: 9cbmmg2cbPG7+3mXBizXNJ1haZ/0FUhztplqsm/dJPJB32okQRAhRWVWyqakJrKjCNVqzT57IZU=), S3 Extended Request ID: 9cbmmg2cbPG7+3mXBizXNJ1haZ/0FUhztplqsm/dJPJB32okQRAhRWVWyqakJrKjCNVqzT57IZU=");
    toThrow.setErrorCode(S3FileSystem.ACCESS_DENIED_ERROR_CODE_V1);
    when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(toThrow);
    fs.setCustomV1Client(new CloseableResource(mockedS3Client, s3 -> {}));
    fs.setInstantNow(Instant.now());
    ContainerAccessDeniedException thrown =
        Assertions.assertThrows(
            ContainerAccessDeniedException.class,
            () -> {
              fs.getUnknownContainer("testunknown");
            });
    Assertions.assertEquals(
        "Cannot find bucket: Access denied to testunknown", thrown.getMessage());
  }

  @Test
  public void testUnknownContainerExistsButNoPermissions() {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final S3Client mockedS3Client = mock(S3Client.class);

    when(mockedS3Client.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(
            S3Exception.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorMessage("Forbidden").build())
                .statusCode(403)
                .build());

    fs.setCustomV2Client(new CloseableRef<>(mockedS3Client));
    fs.setInstantNow(Instant.now());

    ContainerAccessDeniedException exception =
        Assertions.assertThrows(
            ContainerAccessDeniedException.class,
            () -> {
              fs.getUnknownContainer("testunknown");
            });
    Assertions.assertEquals(
        "Cannot find bucket: Access denied to testunknown", exception.getMessage());
  }

  @Test
  public void testUnknownContainerWithoutBucketDiscoveryV1() {
    final Configuration confWithBucketDiscovery = new Configuration();
    confWithBucketDiscovery.setBoolean(DREMIO_ENABLE_BUCKET_DISCOVERY, false);
    confWithBucketDiscovery.set(enableS3V2ClientFeatureFlagName, "false");

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(confWithBucketDiscovery);
    Assertions.assertDoesNotThrow(
        () -> {
          fs.getUnknownContainer("testunknown");
        });
    Assertions.assertEquals(0, fs.getTryFindContainerV1Called());
    Assertions.assertEquals(0, fs.getTryFindContainerV2Called());
  }

  @Test
  public void testUnknownContainerWithoutBucketDiscovery() {
    final Configuration confWithBucketDiscovery = new Configuration();
    confWithBucketDiscovery.setBoolean(DREMIO_ENABLE_BUCKET_DISCOVERY, false);

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem(confWithBucketDiscovery);
    Assertions.assertDoesNotThrow(
        () -> {
          fs.getUnknownContainer("testunknown");
        });
    Assertions.assertEquals(0, fs.getTryFindContainerV1Called());
    Assertions.assertEquals(0, fs.getTryFindContainerV2Called());
  }

  @Test
  public void testVerifyCredentialsRetry() {
    final StsClient mockedClient = mock(StsClient.class);
    final StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
        .thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    final AtomicInteger retryAttemptNo = new AtomicInteger(1);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .then(
            invocationOnMock -> {
              if (retryAttemptNo.incrementAndGet() < 10) {
                throw SdkClientException.builder()
                    .message("Unable to load credentials from service endpoint.")
                    .build();
              }
              return null;
            });

    try (MockedStatic<StsClient> mocked = mockStatic(StsClient.class)) {
      mocked.when(() -> StsClient.builder()).thenReturn(mockedClientBuilder);
      fs.verifyCredentials(new Configuration());
    }
    Assertions.assertEquals(10, retryAttemptNo.get());
  }

  @Test
  public void testVerifyCredentialsNoRetryOnAuthnError() {
    final StsClient mockedClient = mock(StsClient.class);
    final StsClientBuilder mockedClientBuilder = mock(StsClientBuilder.class);
    when(mockedClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
        .thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.region(any(Region.class))).thenReturn(mockedClientBuilder);
    when(mockedClientBuilder.build()).thenReturn(mockedClient);

    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();
    final AtomicInteger retryAttemptNo = new AtomicInteger(0);
    when(mockedClient.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .then(
            invocationOnMock -> {
              retryAttemptNo.incrementAndGet();
              throw StsException.builder()
                  .message(
                      "The security token included in the request is invalid. (Service: Sts, Status Code: 403, Request ID: a7e2e92e-5ebb-4343-87a1-21e4d64edcd4)")
                  .build();
            });

    try (MockedStatic<StsClient> mocked = mockStatic(StsClient.class)) {
      mocked.when(() -> StsClient.builder()).thenReturn(mockedClientBuilder);
      Assertions.assertThrows(
          RuntimeException.class,
          () -> {
            fs.verifyCredentials(new Configuration());
          });
    }
    Assertions.assertEquals(1, retryAttemptNo.get());
  }

  @Test
  public void testUsePathStyleAccess() {
    final Configuration conf = new Configuration();
    Assertions.assertFalse(S3FileSystem.usePathStyleAccess(conf));

    conf.setBoolean("fs.s3a.path.style.access.wrongconfigname", true);
    Assertions.assertFalse(S3FileSystem.usePathStyleAccess(conf));

    conf.setBoolean(Constants.PATH_STYLE_ACCESS, true);
    Assertions.assertTrue(S3FileSystem.usePathStyleAccess(conf));

    conf.setBoolean(Constants.PATH_STYLE_ACCESS, false);
    Assertions.assertFalse(S3FileSystem.usePathStyleAccess(conf));
  }

  @Test
  public void testGetFsClientConsecutiveCallsSameClient() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final CloseableResource<AmazonS3> mockedCloseableS3 = Mockito.mock(CloseableResource.class);
    fs.setCustomV1Client(mockedCloseableS3);
    fs.setInstantNow(Instant.now());

    fs.getS3V1Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getCreateS3V1ClientCalled());

    fs.getS3V1Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getCreateS3V1ClientCalled());
  }

  @Test
  public void testGetFsClientExpiresThenUseNewClientV1() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final CloseableResource<AmazonS3> mockedCloseableS3 = Mockito.mock(CloseableResource.class);
    fs.setCustomV1Client(mockedCloseableS3);

    fs.setInstantNow(Instant.now());
    fs.getS3V1Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getCreateS3V1ClientCalled());

    fs.setInstantNow(Instant.now().plus(65, ChronoUnit.MINUTES));
    fs.getS3V1Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getCreateS3V1ClientCalled());

    fs.setInstantNow(Instant.now().plus(120, ChronoUnit.MINUTES));
    fs.getS3V1Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getCreateS3V1ClientCalled());
  }

  @Test
  public void testGetFsClientExpiresThenUseNewClient() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final CloseableRef<S3Client> mockedCloseableS3 = Mockito.mock(CloseableRef.class);
    fs.setCustomV2Client(mockedCloseableS3);

    fs.setInstantNow(Instant.now());
    fs.getFsS3Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getFactoryCreateS3ClientCalled());

    fs.setInstantNow(Instant.now().plus(65, ChronoUnit.MINUTES));
    fs.getFsS3Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getFactoryCreateS3ClientCalled());

    fs.setInstantNow(Instant.now().plus(120, ChronoUnit.MINUTES));
    fs.getFsS3Client();
    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getFactoryCreateS3ClientCalled());
  }

  @Test
  public void testGetFsClientConcurrentAccessV1() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final CloseableResource<AmazonS3> mockedCloseableS3 = Mockito.mock(CloseableResource.class);
    fs.setCustomV1Client(mockedCloseableS3);
    fs.setInstantNow(Instant.now());

    final int nThreads = 32;
    final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    final List<Future<?>> futures = new ArrayList<>();
    IntStream.range(0, 5 * nThreads)
        .forEach(
            i -> {
              Future<?> future =
                  executorService.submit(
                      () -> {
                        try {
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              futures.add(future);
            });

    // Ensuring the threads did not throw
    for (Future<?> future : futures) {
      future.get();
    }

    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getCreateS3V1ClientCalled());

    fs.setInstantNow(Instant.now().plus(65, ChronoUnit.MINUTES));

    futures.clear();
    IntStream.range(0, 5 * nThreads)
        .forEach(
            i -> {
              Future<?> future =
                  executorService.submit(
                      () -> {
                        try {
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                          fs.getS3V1Client();
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              futures.add(future);
            });

    // Ensuring the threads did not throw
    for (Future<?> future : futures) {
      future.get();
    }

    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getCreateS3V1ClientCalled());
  }

  @Test
  public void testGetFsClientConcurrentAccess() throws Exception {
    final TestExtendedS3FileSystem fs = new TestExtendedS3FileSystem();

    final CloseableRef<S3Client> mockedCloseableS3 = Mockito.mock(CloseableRef.class);
    fs.setCustomV2Client(mockedCloseableS3);
    fs.setInstantNow(Instant.now());

    final int nThreads = 32;
    final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    final List<Future<?>> futures = new ArrayList<>();
    IntStream.range(0, 5 * nThreads)
        .forEach(
            i -> {
              Future<?> future =
                  executorService.submit(
                      () -> {
                        try {
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              futures.add(future);
            });

    // Ensuring the threads did not throw
    for (Future<?> future : futures) {
      future.get();
    }

    Mockito.verify(mockedCloseableS3, Mockito.times(0)).close();
    Assertions.assertEquals(1, fs.getFactoryCreateS3ClientCalled());

    fs.setInstantNow(Instant.now().plus(65, ChronoUnit.MINUTES));

    futures.clear();
    IntStream.range(0, 5 * nThreads)
        .forEach(
            i -> {
              Future<?> future =
                  executorService.submit(
                      () -> {
                        try {
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                          fs.getFsS3Client();
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              futures.add(future);
            });

    // Ensuring the threads did not throw
    for (Future<?> future : futures) {
      future.get();
    }

    Mockito.verify(mockedCloseableS3, Mockito.times(1)).close();
    Assertions.assertEquals(2, fs.getFactoryCreateS3ClientCalled());
  }

  @Test
  void testConfigureS3ClientFactoryFailOnBeingCalledTwice() throws Exception {
    S3FileSystem fs = new S3FileSystem();

    Configuration conf = new Configuration();
    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, S3StoragePlugin.NONE_PROVIDER);
    fs.setConf(conf);

    fs.configureS3ClientFactory(conf);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              fs.configureS3ClientFactory(conf);
            });

    Assertions.assertEquals(
        String.format(
            "The S3ClientFactory should only be configured once. "
                + "(s3ClientProperties == null) -> %s, (s3ClientFactory == null) -> %s, "
                + "(fsS3Client == null) -> %s, (asyncClientCache.size()) -> %s",
            false, false, true, 0),
        exception.getMessage());
  }

  @Test
  public void testBuildRegionEndpointSuccess() {
    for (Region region : Region.regions()) {
      String endpoint = S3FileSystem.buildRegionEndpoint(region.id());

      String expected =
          String.format(
              "s3.%samazonaws.com", region.id().equals("us-east-1") ? "" : region.id() + ".");

      Assertions.assertEquals(expected, endpoint);
    }
  }

  @Test
  public void testBuildRegionEndpointInvalidRegion() {
    List<String> invalidRegions =
        List.of("US-WEST-1", "US-EAST-1", "US_EAST_1", "us_east_1", "not_a_region", "test");

    for (String region : invalidRegions) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> S3FileSystem.buildRegionEndpoint(region));
      Assertions.assertEquals(
          String.format("%s is not a valid AWS region.", region), exception.getMessage());
    }
  }

  private AccessControlList getAcl(final AmazonS3 s3Client) {
    final ArrayList<Grant> grantCollection = new ArrayList<>();

    // Grant the account owner full control.
    final Grant grant1 =
        new Grant(
            new CanonicalGrantee(s3Client.getS3AccountOwner().getId()), Permission.FullControl);
    grantCollection.add(grant1);

    // Save grants by replacing all current ACL grants with the two we just created.
    final AccessControlList bucketAcl = new AccessControlList();
    bucketAcl.grantAllPermissions(grantCollection.toArray(new Grant[0]));
    return bucketAcl;
  }

  public static class TestExtendedS3AFileSystem extends S3AFileSystem {
    private final String bucketName;
    private boolean fsWasClosed;

    public TestExtendedS3AFileSystem(String bucketName) {
      this.bucketName = bucketName;
      fsWasClosed = false;
    }

    @Override
    public void close() throws IOException {
      fsWasClosed = true;
    }

    @Override
    public FileStatus getFileStatus(Path f) {
      return new FileStatus(0, false, 0, 0, 0, new Path("/" + bucketName));
    }

    public boolean wasFSClosed() {
      return fsWasClosed;
    }
  }

  private static final class TestExtendedS3FileSystem extends S3FileSystem {
    private CloseableResource<AmazonS3> s3V1Closeable;
    private CloseableRef<S3Client> s3V2Closeable;
    private Instant now;
    private int createS3V1ClientCalled = 0;
    private int factoryCreateS3ClientCalled = 0;
    private int tryFindContainerV1Called = 0;
    private int tryFindContainerV2Called = 0;

    public TestExtendedS3FileSystem() {
      final Configuration conf = new Configuration();
      setConf(conf);
    }

    public TestExtendedS3FileSystem(Configuration conf) {
      setConf(conf);
    }

    void setCustomV2Client(CloseableRef<S3Client> s3V2Closeable) {
      this.s3V2Closeable = s3V2Closeable;
      this.s3ClientFactory = mock(S3ClientFactory.class);
      when(this.s3ClientFactory.createS3Client())
          .thenAnswer(
              invocation -> {
                ++factoryCreateS3ClientCalled;
                return this.s3V2Closeable;
              });
    }

    @Override
    void configureS3ClientFactory(Configuration config) {}

    void setCustomV1Client(CloseableResource<AmazonS3> s3V1Closeable) {
      this.s3V1Closeable = s3V1Closeable;
    }

    @Override
    CloseableResource<AmazonS3> createS3V1ClientNonStatic() throws Exception {
      ++createS3V1ClientCalled;
      return s3V1Closeable;
    }

    void setInstantNow(Instant now) {
      this.now = now;
    }

    int getCreateS3V1ClientCalled() {
      return createS3V1ClientCalled;
    }

    int getFactoryCreateS3ClientCalled() {
      return factoryCreateS3ClientCalled;
    }

    int getTryFindContainerV1Called() {
      return tryFindContainerV1Called;
    }

    int getTryFindContainerV2Called() {
      return tryFindContainerV2Called;
    }

    @Override
    Instant getInstantNowNonStatic() {
      return now;
    }

    @Override
    public AwsCredentialsProvider getAsync2Provider(Configuration conf) {
      AwsCredentialsProvider mockProvider = mock(AwsCredentialsProvider.class);
      return mockProvider;
    }

    @Override
    void tryFindContainerV1(String containerName) throws IOException {
      ++tryFindContainerV1Called;
      super.tryFindContainerV1(containerName);
    }

    @Override
    void tryFindContainerV2(String containerName) throws IOException {
      ++tryFindContainerV2Called;
      super.tryFindContainerV2(containerName);
    }

    @Override
    protected boolean isRequesterPays() {
      return false;
    }
  }
}
