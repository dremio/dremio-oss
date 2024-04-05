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

import com.dremio.common.exceptions.UserException;
import com.dremio.plugins.util.ContainerFileSystem.ContainerCreator;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A ContainerProvider that leverages v8 Azure APIs to list Blob Containers */
abstract class BaseBlobContainerProvider implements ContainerProvider {
  private static final Logger logger = LoggerFactory.getLogger(BaseBlobContainerProvider.class);

  private final AzureStorageFileSystem parent;
  private final ImmutableList<String> containers;

  private final URI connection;
  private final String account;

  BaseBlobContainerProvider(
      AzureStorageFileSystem parent, URI connection, String account, String[] containers) {
    this.parent = parent;
    this.connection = connection;
    this.account = account;

    this.containers = containers != null ? ImmutableList.copyOf(containers) : ImmutableList.of();
  }

  protected URI getConnection() {
    return connection;
  }

  protected String getAccount() {
    return account;
  }

  protected abstract CloudBlobClient getCloudBlobClient();

  @Override
  public Stream<ContainerCreator> getContainerCreators() {
    if (containers.isEmpty()) {
      return StreamSupport.stream(getCloudBlobClient().listContainers().spliterator(), false)
          .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c.getName()));
    } else {
      return containers.stream()
          .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c));
    }
  }

  @Override
  public void verfiyContainersExist() {
    List<String> list = containers.asList();
    for (String c : list) {
      try {
        logger.debug("Exists validation for whitelisted azure container {}:{}", account, c);
        assertContainerExists(c);
      } catch (AzureStoragePluginException e) {
        throw UserException.validationError()
            .message(
                "Failure while validating existence of container %s. Error %s",
                c, e.getCause().getMessage())
            .buildSilently();
      }
    }
  }

  @Override
  public void assertContainerExists(final String containerName) {
    boolean exists;
    try {
      exists = getCloudBlobClient().getContainerReference(containerName).exists();
    } catch (StorageException e) {
      throw new AzureStoragePluginException(
          String.format(
              "Error occurred %s while checking for existence of container %s",
              e.getMessage(), containerName));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format(
              "Error response %s while checking for existence of container %s",
              e.getMessage(), containerName));
    }

    if (!exists) {
      throw new AzureStoragePluginException(
          (String.format("Unable to find container %s", containerName)));
    }
  }
}
