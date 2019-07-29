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
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.util.ContainerFileSystem.ContainerCreator;
import com.google.common.collect.AbstractIterator;
import com.microsoft.azure.storage.v10.adlsg2.models.Filesystem;
import com.microsoft.azure.storage.v10.adlsg2.models.FilesystemListResponse;

/**
 * Container provider for hierarchical storage accounts.
 */
class FsV10Provider implements ContainerProvider {

  private static final Logger logger = LoggerFactory.getLogger(AzureStorageFileSystem.class);

  private final AzureStorageFileSystem parent;
  private final DataLakeG2Client client;

  public FsV10Provider(
      DataLakeG2Client client,
      AzureStorageFileSystem parent) throws IOException {
    super();
    this.parent = parent;
    this.client = client;
  }

  @Override
  public Stream<ContainerCreator> getContainerCreators() throws IOException {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            new ContainerIter(),
            Spliterator.ORDERED),
        false);
  }

  private final class ContainerIter extends AbstractIterator<ContainerCreator> {

    private FilesystemListResponse response;
    private Iterator<Filesystem> iterator;

    @Override
    protected ContainerCreator computeNext() {
      if (response == null) {
        try {
          response = client.listFilesystems(null, null).blockingGet();
        } catch(Exception ex) {
          logger.error("Unable to list file systems with ADLSg2 client", ex);
          return endOfData();
        }
        iterator = response.body().filesystems().iterator();
      }

      while (!iterator.hasNext()) {
        if (response.headers().xMsContinuation() == null || response.headers().xMsContinuation().isEmpty()) {
          return endOfData();
        }
        try {
          response = client.listFilesystems(null, response.headers().xMsContinuation()).blockingGet();
        } catch(Exception ex) {
          logger.error("Unable to list file systems with ADLSg2 client", ex);
          return endOfData();
        }
        iterator = response.body().filesystems().iterator();
      }

      return new AzureStorageFileSystem.ContainerCreatorImpl(parent, iterator.next().name());
    }

  }

}
