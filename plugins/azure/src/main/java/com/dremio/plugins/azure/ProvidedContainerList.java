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
import java.util.stream.Stream;

import com.dremio.plugins.util.ContainerFileSystem.ContainerCreator;
import com.google.common.collect.ImmutableList;

/**
 * A ContainerProvider that uses a defined list of containers.
 */
class ProvidedContainerList implements ContainerProvider {

  private final ImmutableList<String> containers;
  private final AzureStorageFileSystem parent;

  public ProvidedContainerList(AzureStorageFileSystem parent, String[] containers) throws IOException {
    this.parent = parent;
    this.containers = ImmutableList.copyOf(containers);
  }

  @Override
  public Stream<ContainerCreator> getContainerCreators() throws IOException {
    return containers.stream()
        .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c));
  }
}
