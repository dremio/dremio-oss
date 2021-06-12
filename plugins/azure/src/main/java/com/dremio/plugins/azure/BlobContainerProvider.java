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
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.plugins.util.ContainerFileSystem.ContainerCreator;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;

/**
 * A ContainerProvider that leverages v8 Azure APIs to list Blob Containers
 */
class BlobContainerProvider implements ContainerProvider {
  private static final Logger logger = LoggerFactory.getLogger(BlobContainerProvider.class);

  private CloudBlobClient cloudBlobClient;
  private final AzureStorageFileSystem parent;
  private final URI connection;
  private final String account;
  private ImmutableList<String> containers = ImmutableList.of();

  public BlobContainerProvider(AzureStorageFileSystem parent, String connection, String account, String key) throws IOException {
    this(parent, account, connection, new StorageCredentialsAccountAndKey(account, key), false, null);
  }


  public BlobContainerProvider(AzureStorageFileSystem parent, String connection, String account, String key, String[] containers) throws IOException {
    this(parent, account, connection, new StorageCredentialsAccountAndKey(account, key), false, containers);
  }

  protected BlobContainerProvider(AzureStorageFileSystem parent, String account, String connection, StorageCredentials credentials, boolean useAzureAD, String[] containers) throws IOException {
    this.parent = parent;
    try {
      this.account = account;
      this.connection = new URI(connection);
      cloudBlobClient = new CloudBlobClient(this.connection, credentials);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    if(containers != null) {
      this.containers = ImmutableList.copyOf(containers);
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
    if(containers.isEmpty()) {
      return StreamSupport
        .stream(cloudBlobClient.listContainers().spliterator(), false)
        .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c.getName()));
    } else {
      return containers.stream()
        .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c));
    }
  }

  public void verfiyContainersExist() {
    List<String> list = containers.asList();
    for(String c : list) {
      try {
        logger.debug("Exists validation for whitelisted azure container " + account + ":" + c);
        assertContainerExists(c);
      } catch (AzureStoragePluginException e) {
        throw UserException.validationError()
          .message(String.format("Failure while validating existence of container %s. Error %s", c, e.getCause().getMessage()))
          .build();
      }
    }
  }

  @Override
  public void assertContainerExists(final String containerName) {
    boolean exists;
    try {
      exists = cloudBlobClient.getContainerReference(containerName).exists();
    } catch (StorageException e) {
      throw new AzureStoragePluginException(String.format("Error occurred %s while checking for existence of container %s", e.getMessage(), containerName));
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Error response %s while checking for existence of container %s", e.getMessage(), containerName));
    }

    if(!exists) {
      throw new AzureStoragePluginException((String.format("Unable to find container %s", containerName)));
    }
  }
}
