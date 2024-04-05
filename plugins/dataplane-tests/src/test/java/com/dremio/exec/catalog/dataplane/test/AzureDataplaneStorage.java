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

import static com.dremio.plugins.azure.AzureAuthenticationType.ACCESS_KEY;
import static com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType.AZURE;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.google.common.base.Preconditions;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public class AzureDataplaneStorage implements DataplaneStorage {

  private static final String AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME =
      Preconditions.checkNotNull(System.getenv("AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME"));
  private static final String AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY =
      Preconditions.checkNotNull(System.getenv("AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY"));

  private final String primaryBucketName = "testdataplanebucket" + DataplaneTestDefines.uniqueInt();
  private final String alternateBucketName =
      "testalternatebucket" + DataplaneTestDefines.uniqueInt();

  private BlobServiceClient azureClient;

  @Override
  public void start() {
    azureClient =
        new BlobServiceClientBuilder()
            .credential(
                new StorageSharedKeyCredential(
                    AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME, AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY))
            .endpoint(
                String.format(
                    "https://%s.blob.core.windows.net/", AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME))
            .buildClient();

    azureClient.createBlobContainerIfNotExists(primaryBucketName);
    azureClient.createBlobContainerIfNotExists(alternateBucketName);
  }

  @Override
  public StorageType getType() {
    return StorageType.AZURE;
  }

  @Override
  public void close() throws Exception {
    azureClient.deleteBlobContainerIfExists(primaryBucketName);
    azureClient.deleteBlobContainerIfExists(alternateBucketName);
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
    return azureClient
        .getBlobContainerClient(getBucketName(bucketSelection))
        .getBlobClient(stripPrefix(bucketSelection, objectPath))
        .exists();
  }

  @Override
  public void putObject(BucketSelection bucketSelection, String objectPath, File file) {
    azureClient
        .getBlobContainerClient(getBucketName(bucketSelection))
        .getBlobClient(objectPath)
        .uploadFromFile(file.getPath());
  }

  @Override
  public void deleteObject(BucketSelection bucketSelection, String objectPath) {
    azureClient
        .getBlobContainerClient(getBucketName(bucketSelection))
        .getBlobClient(objectPath)
        .delete();
  }

  @Override
  public void deleteObjects(BucketSelection bucketSelection, List<String> objectPaths) {
    for (String objectPath : objectPaths) {
      deleteObject(bucketSelection, objectPath);
    }
  }

  @Override
  public Stream<String> listObjectNames(
      BucketSelection bucketSelection, String filterPath, Predicate<String> objectNameFilter) {
    return azureClient
        .getBlobContainerClient(getBucketName(bucketSelection))
        .listBlobs(new ListBlobsOptions().setPrefix(filterPath), Duration.ofSeconds(30))
        .stream()
        .map(BlobItem::getName)
        .filter(objectNameFilter);
  }

  @Override
  public NessiePluginConfig preparePluginConfig(
      BucketSelection bucketSelection, String nessieEndpoint) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieEndpoint;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = true;

    nessiePluginConfig.storageProvider = AZURE;
    nessiePluginConfig.azureStorageAccount = AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME;
    nessiePluginConfig.azureRootPath = getBucketName(bucketSelection);
    nessiePluginConfig.azureAuthenticationType = ACCESS_KEY;
    nessiePluginConfig.azureAccessKey = SecretRef.of(AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY);

    return nessiePluginConfig;
  }

  private String stripPrefix(BucketSelection bucketSelection, String objectPath) {
    final String objectPathWithoutScheme = StringUtils.removeStart(objectPath, "wasbs://");
    final String objectPathWithoutSchemeOrBucket =
        StringUtils.removeStart(objectPathWithoutScheme, getBucketName(bucketSelection));
    final String objectPathWithoutSchemeOrBucketOrServer =
        StringUtils.substringAfter(objectPathWithoutSchemeOrBucket, "blob.core.windows.net");
    return StringUtils.removeStart(objectPathWithoutSchemeOrBucketOrServer, "/");
  }
}
