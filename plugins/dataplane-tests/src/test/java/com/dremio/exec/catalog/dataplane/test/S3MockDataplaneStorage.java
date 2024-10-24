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
package com.dremio.exec.catalog.dataplane.test;

import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.s3.store.S3FileSystem;
import io.findify.s3mock.S3Mock;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3MockDataplaneStorage implements DataplaneStorage {

  private static AmazonS3 s3Client;
  private static S3Mock s3MockServer;
  private static int s3Port;

  private final String primaryBucketName = "testdataplanebucket" + DataplaneTestDefines.uniqueInt();
  private final String alternateBucketName =
      "testalternatebucket" + DataplaneTestDefines.uniqueInt();

  @Override
  public void start() {
    // We use S3Mock's in-memory backend implementation to avoid incompatibility issues between
    // Hadoop's S3's implementation and S3Mock's filesystem backend. When doing file deletions,
    // Hadoop executes a "maybeCreateFakeParentDirectory" operation that tries to write a 0 byte
    // object to S3. S3Mock's filesystem backend throws an AmazonS3Exception with a "Is a directory"
    // message. The in-memory backend does not have the same issue. We encountered this problem (in
    // tests only, not AWS S3) when cleaning up Iceberg metadata files after a failed Nessie commit.
    s3MockServer = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();

    s3Port = s3MockServer.start().localAddress().getPort();

    AwsClientBuilder.EndpointConfiguration endpoint =
        new AwsClientBuilder.EndpointConfiguration(
            String.format("http://localhost:%d", s3Port), Region.US_EAST_1.toString());

    s3Client =
        AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    s3Client.createBucket(primaryBucketName);
    s3Client.createBucket(alternateBucketName);
  }

  @Override
  public StorageType getType() {
    return StorageType.AWS_S3_MOCK;
  }

  @Override
  public void close() throws Exception {
    s3Client.shutdown();

    if (s3MockServer != null) {
      s3MockServer.shutdown();
      s3MockServer = null;
    }
  }

  @Override
  public int getPort() {
    return s3Port;
  }

  @Override
  public String getBucketName(BucketSelection bucketSelection) {
    switch (bucketSelection) {
      case PRIMARY_BUCKET:
        return primaryBucketName;
      case ALTERNATE_BUCKET:
        return alternateBucketName;
      default:
        throw new IllegalStateException("Unexpected value: " + bucketSelection);
    }
  }

  @Override
  public boolean doesObjectExist(BucketSelection bucketSelection, String objectPath) {
    return s3Client.doesObjectExist(
        getBucketName(bucketSelection), stripPrefix(bucketSelection, objectPath));
  }

  @Override
  public void putObject(String objectPath, File file) {
    final AmazonS3URI objectFileUri = new AmazonS3URI(objectPath);
    s3Client.putObject(objectFileUri.getBucket(), objectFileUri.getKey(), file);
  }

  @Override
  public void deleteObject(BucketSelection bucketSelection, String objectPath) {
    s3Client.deleteObject(getBucketName(bucketSelection), objectPath);
  }

  @Override
  public void deleteObjects(BucketSelection bucketSelection, List<String> objectPaths) {
    s3Client.deleteObjects(
        new DeleteObjectsRequest(getBucketName(bucketSelection))
            .withKeys(
                objectPaths.stream()
                    .map(DeleteObjectsRequest.KeyVersion::new)
                    .collect(Collectors.toList())));
  }

  @Override
  public Stream<String> listObjectNames(
      BucketSelection bucketSelection, String filterPath, Predicate<String> objectNameFilter) {
    return s3Client
        .listObjects(getBucketName(bucketSelection), String.join("/", filterPath))
        .getObjectSummaries()
        .stream()
        .map(S3ObjectSummary::getKey)
        .filter(objectNameFilter);
  }

  @Override
  public NessiePluginConfig prepareNessiePluginConfig(
      BucketSelection bucketSelection, String nessieEndpoint) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieEndpoint;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.credentialType =
        AWSAuthenticationType.ACCESS_KEY; // Unused, just needs to be set
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = SecretRef.of("bar"); // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = getBucketName(bucketSelection);

    // S3Mock settings
    nessiePluginConfig.propertyList =
        Arrays.asList(
            new Property("fs.s3a.endpoint", "localhost:" + getPort()),
            new Property("fs.s3a.path.style.access", "true"),
            new Property("fs.s3a.connection.ssl.enabled", "false"),
            new Property("fs.s3.impl", TestExtendedS3AFilesystem.class.getName()),
            new Property("fs.s3a.impl", TestExtendedS3AFilesystem.class.getName()),
            new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));

    return nessiePluginConfig;
  }

  @Override
  public FileIO getFileIO() {
    AwsCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar"));

    URI endpointUri = URI.create("http://localhost:" + getPort());
    S3FileIO fileIO =
        new S3FileIO(
            () ->
                S3Client.builder()
                    .endpointOverride(endpointUri)
                    .region(Region.US_EAST_1)
                    .credentialsProvider(credentialsProvider)
                    .build());
    fileIO.initialize(Collections.emptyMap());
    return fileIO;
  }

  @Override
  public String getWarehousePath() {
    return "s3://" + getBucketName(PRIMARY_BUCKET);
  }

  private String stripPrefix(BucketSelection bucketSelection, String objectPath) {
    final String objectPathWithoutScheme = StringUtils.removeStart(objectPath, "s3://");
    final String objectPathWithoutSchemeOrBucket =
        StringUtils.removeStart(objectPathWithoutScheme, getBucketName(bucketSelection));
    return StringUtils.removeStart(objectPathWithoutSchemeOrBucket, "/");
  }
}
