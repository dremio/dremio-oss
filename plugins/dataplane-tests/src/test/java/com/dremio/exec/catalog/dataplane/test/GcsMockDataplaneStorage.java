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
import static com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType.GOOGLE;
import static com.dremio.plugins.gcs.GoogleBucketFileSystem.DREMIO_BYPASS_AUTH_CONFIG_FOR_TESTING_WITH_URL;
import static org.apache.iceberg.gcp.GCPProperties.GCS_PROJECT_ID;
import static org.apache.iceberg.gcp.GCPProperties.GCS_SERVICE_HOST;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.gcs.GCSConf.AuthMode;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;

public class GcsMockDataplaneStorage implements DataplaneStorage {

  protected static final String TESTING_PROJECT_ID = "gcsTestProjectId";

  private static Storage gcsClient;
  private static FakeGcsServer gcsMockServer;

  private final String primaryBucketName = "testdataplanebucket" + DataplaneTestDefines.uniqueInt();
  private final String alternateBucketName =
      "testalternatebucket" + DataplaneTestDefines.uniqueInt();

  @Override
  public void start() {
    gcsMockServer = FakeGcsServer.startServer();

    gcsClient =
        HttpStorageOptions.newBuilder()
            .setHost(getGcsMockServerUrl())
            .setProjectId(TESTING_PROJECT_ID)
            .build()
            .getService();

    gcsClient.create(BucketInfo.of(primaryBucketName));
    gcsClient.create(BucketInfo.of(alternateBucketName));
  }

  @Override
  public StorageType getType() {
    return StorageType.GCS_MOCK;
  }

  @Override
  public int getPort() {
    return gcsMockServer.getHttpPort();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(gcsClient, gcsMockServer);
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
    return gcsClient.get(blobIdFor(bucketSelection, objectPath)) != null;
  }

  @Override
  public void putObject(String objectPath, File file) {
    // TODO: Derive bucket from the path
    BlobInfo blobInfo = BlobInfo.newBuilder(blobIdFor(PRIMARY_BUCKET, objectPath)).build();
    try {
      gcsClient.create(blobInfo, Files.readAllBytes(file.toPath()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteObject(BucketSelection bucketSelection, String objectPath) {
    gcsClient.delete(blobIdFor(bucketSelection, objectPath));
  }

  @Override
  public void deleteObjects(BucketSelection bucketSelection, List<String> objectPaths) {
    BlobId[] blobIds =
        objectPaths.stream().map(op -> blobIdFor(bucketSelection, op)).toArray(BlobId[]::new);
    gcsClient.delete(blobIds);
  }

  @Override
  public Stream<String> listObjectNames(
      BucketSelection bucketSelection, String filterPath, Predicate<String> objectNameFilter) {
    Iterable<Blob> blobsInAllBuckets =
        gcsClient
            .list(getBucketName(bucketSelection), BlobListOption.prefix(filterPath))
            .iterateAll();
    return StreamSupport.stream(blobsInAllBuckets.spliterator(), false)
        .map(Blob::getName)
        .filter(objectNameFilter);
  }

  @Override
  public NessiePluginConfig prepareNessiePluginConfig(
      BucketSelection bucketSelection, String nessieEndpoint) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieEndpoint;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;

    nessiePluginConfig.storageProvider = GOOGLE;
    nessiePluginConfig.googleProjectId = TESTING_PROJECT_ID;
    nessiePluginConfig.googleAuthenticationType = AuthMode.SERVICE_ACCOUNT_KEYS;
    nessiePluginConfig.googlePrivateKeyId = "unusedPrivateKeyId"; // Unused, just needs to be set
    nessiePluginConfig.googlePrivateKey = // Unused, just needs to be set
        SecretRef.of( // Not a real key
            "-----BEGIN PRIVATE KEY-----\n"
                + "MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEAq7BFUpkGp3+LQmlQ\n"
                + "Yx2eqzDV+xeG8kx/sQFV18S5JhzGeIJNA72wSeukEPojtqUyX2J0CciPBh7eqclQ\n"
                + "2zpAswIDAQABAkAgisq4+zRdrzkwH1ITV1vpytnkO/NiHcnePQiOW0VUybPyHoGM\n"
                + "/jf75C5xET7ZQpBe5kx5VHsPZj0CBb3b+wSRAiEA2mPWCBytosIU/ODRfq6EiV04\n"
                + "lt6waE7I2uSPqIC20LcCIQDJQYIHQII+3YaPqyhGgqMexuuuGx+lDKD6/Fu/JwPb\n"
                + "5QIhAKthiYcYKlL9h8bjDsQhZDUACPasjzdsDEdq8inDyLOFAiEAmCr/tZwA3qeA\n"
                + "ZoBzI10DGPIuoKXBd3nk/eBxPkaxlEECIQCNymjsoI7GldtujVnr1qT+3yedLfHK\n"
                + "srDVjIT3LsvTqw==\n"
                + "-----END PRIVATE KEY-----");
    nessiePluginConfig.googleClientEmail = "unusedClientEmail"; // Unused, just needs to be set
    nessiePluginConfig.googleClientId = "unusedClientId"; // Unused, just needs to be set
    nessiePluginConfig.googleRootPath = getBucketName(bucketSelection);

    // GCS Mock settings
    nessiePluginConfig.propertyList =
        Arrays.asList(
            new Property("fs.gs.storage.root.url", getGcsMockServerUrl()),
            new Property(
                "fs.gs.token.server.url",
                getGcsMockServerUrl()), // Not needed, but helps to make sure we don't accidentally
            // talk to a live GCS
            new Property(
                "fs.gs.auth.access.token.provider.impl",
                "com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider"),
            new Property(DREMIO_BYPASS_AUTH_CONFIG_FOR_TESTING_WITH_URL, getGcsMockServerUrl()));

    return nessiePluginConfig;
  }

  @Override
  public FileIO getFileIO() {
    GCSFileIO gcsFileIO = new GCSFileIO();
    gcsFileIO.initialize(
        ImmutableMap.of(
            GCS_PROJECT_ID, TESTING_PROJECT_ID, GCS_SERVICE_HOST, getGcsMockServerUrl()));
    return gcsFileIO;
  }

  @Override
  public String getWarehousePath() {
    return "gs://" + getBucketName(PRIMARY_BUCKET);
  }

  protected String getGcsMockServerUrl() {
    return "http://localhost:" + getPort();
  }

  private BlobId blobIdFor(BucketSelection bucketSelection, String objectPath) {
    return BlobId.of(getBucketName(bucketSelection), stripPrefix(bucketSelection, objectPath));
  }

  private String stripPrefix(BucketSelection bucketSelection, String objectPath) {
    final String objectPathWithoutScheme = StringUtils.removeStart(objectPath, "gs://");
    final String objectPathWithoutSchemeOrBucket =
        StringUtils.removeStart(objectPathWithoutScheme, getBucketName(bucketSelection));
    return StringUtils.removeStart(objectPathWithoutSchemeOrBucket, "/");
  }
}
