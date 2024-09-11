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
package com.dremio.service.listing;

import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Implementation of {@link DatasetListingService} that interacts with {@link NamespaceService}
 * running on this node.
 */
@Singleton
public class DatasetListingServiceImpl implements DatasetListingService {

  private final Provider<NamespaceService.Factory> factoryProvider;

  @Inject
  public DatasetListingServiceImpl(Provider<NamespaceService.Factory> factoryProvider) {
    this.factoryProvider = factoryProvider;
  }

  @Override
  public void start() {
    // no op
  }

  @Override
  public void close() {
    // no op
  }

  @Override
  public List<SourceConfig> getSources(String username) {
    return factoryProvider.get().get(username).getSources();
  }

  @Override
  public SourceConfig getSource(String username, String sourcename) throws NamespaceException {
    return factoryProvider.get().get(username).getSource(new NamespaceKey(sourcename));
  }
}
