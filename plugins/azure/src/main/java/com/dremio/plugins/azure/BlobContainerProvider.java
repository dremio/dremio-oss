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
package com.dremio.plugins.azure;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dremio.plugins.util.ContainerFileSystem.ContainerCreator;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlobClient;

/**
 * A ContainerProvider that leverages v8 Azure APIs to list Blob Containers
 */
class BlobContainerProvider implements ContainerProvider {

  private CloudBlobClient cloudBlobClient;
  private final AzureStorageFileSystem parent;
  private final URI connection;
  private final String account;

  public BlobContainerProvider(AzureStorageFileSystem parent, String connection, String account, String key) throws IOException {
    this(parent, account, connection, new StorageCredentialsAccountAndKey(account, key), false);
  }

  protected BlobContainerProvider(AzureStorageFileSystem parent, String account, String connection, StorageCredentials credentials, boolean useAzureAD) throws IOException {
    this.parent = parent;
    try {
      this.account = account;
      this.connection = new URI(connection);
      cloudBlobClient = new CloudBlobClient(this.connection, credentials);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  protected String getAccount() {
    return this.account;
  }

  protected URI getConnection() {
    return this.connection;
  }

  protected void setCloudBlobClient(CloudBlobClient cloudBlobClient) {
    this.cloudBlobClient = cloudBlobClient;
  }

  @Override
  public Stream<ContainerCreator> getContainerCreators() throws IOException {
    return StreamSupport
        .stream(cloudBlobClient.listContainers().spliterator(), false)
        .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c.getName()));
  }
}
